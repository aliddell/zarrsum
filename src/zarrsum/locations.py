"""Resolve retained release stores without making their locations part of identity.

Location records follow Hermes. Portable Reef locations stay under the shared projects root. Callers supply the local storage platform.
"""

from __future__ import annotations

from contextlib import closing

import hashlib
import json
from pathlib import Path
import posixpath
import re
import tempfile
import time
from urllib.parse import urlsplit

from botocore.exceptions import BotoCoreError, ClientError

from .releases import Store, atomic_file, canonical, digest, identifier, member
from .graph_index import Workspace
from .remotes import (
    Aliases,
    Boundary,
    graph,
    local_get,
    remote_config,
    validate,
    verified_checkout,
)

REFERENCE_BYTES = 64 << 10
MAX_LOCATIONS = 16
MAX_FETCH_BYTES = 2 << 30
LOCAL_PLATFORMS = {"reef", "sf_hpc"}


class CandidateError(ValueError):
    """A location cannot supply verified content; another location may be tried."""


class CacheError(ValueError):
    """Retained cache bytes are corrupt; the owner must repair them explicitly."""


def read_json(path, limit=REFERENCE_BYTES):
    with Path(path).open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("configuration exceeds byte limit")

    def pairs(values):
        found = {}
        for key, value in values:
            if key in found:
                raise ValueError("duplicate JSON field")
            found[key] = value
        return found

    return json.loads(raw, object_pairs_hook=pairs)


def location(value):
    if not isinstance(value, dict) or set(value) != {
        "storage_platform",
        "location_uri",
    }:
        raise ValueError("expected Hermes storage_platform and location_uri fields")
    platform, uri = value["storage_platform"], value["location_uri"]
    if not isinstance(platform, str) or platform not in LOCAL_PLATFORMS | {"s3"}:
        raise ValueError("unsupported storage platform")
    if (
        not isinstance(uri, str)
        or not 1 <= len(uri) <= 4096
        or any(ord(char) < 32 for char in uri)
        or "\\" in uri
    ):
        raise ValueError("invalid location URI")
    scheme, separator, rest = uri.partition("://")
    if not separator or scheme != platform or not rest or "?" in rest or "#" in rest:
        raise ValueError("location platform and URI do not match")
    if urlsplit(uri).username is not None:
        raise ValueError("location URI must not contain credentials")
    if platform == "s3":
        parsed = urlsplit(uri)
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", parsed.netloc)
            or "@" in parsed.netloc
            or ":" in parsed.netloc
            or not parsed.path.startswith("/")
            or parsed.path.startswith("//")
        ):
            raise ValueError(
                "expected an S3 bucket and store prefix without credentials"
            )
        member(parsed.path[1:].rstrip("/"))
    else:
        if platform == "reef":
            if rest.startswith("/"):
                raise ValueError("use the portable reef:// projects-relative form")
            member(rest.rstrip("/"))
            path = "/mnt/main0/projects/" + rest
            normalized = posixpath.normpath(path)
            if not normalized.startswith("/mnt/main0/projects/"):
                raise ValueError("Reef store must remain under the projects root")
        elif not rest.startswith("/"):
            raise ValueError("HPC release stores need absolute paths")
    return {
        "storage_platform": platform,
        "location_uri": scheme + "://" + (rest.rstrip("/") or "/"),
    }


def filesystem_root(value, local_platform):
    value = location(value)
    platform = value["storage_platform"]
    if platform not in LOCAL_PLATFORMS or platform != local_platform:
        raise CandidateError("filesystem is not local to this execution platform")
    rest = value["location_uri"].partition("://")[2]
    if platform == "reef":
        rest = rest if rest.startswith("/") else "/mnt/main0/projects/" + rest
        if rest == "/bio/projects" or rest.startswith("/bio/projects/"):
            rest = "/mnt/main0/projects" + rest[len("/bio/projects") :]
    path = Path(rest).resolve()
    if platform == "reef" and not path.is_relative_to("/mnt/main0/projects"):
        raise CandidateError("Reef store resolves outside the projects root")
    return path


def reference(value):
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "release_id", "locations"}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("invalid release reference")
    identifier(value["release_id"])
    if (
        not isinstance(value["locations"], list)
        or len(value["locations"]) > MAX_LOCATIONS
    ):
        raise ValueError("expected at most 16 candidate locations")
    locations = [location(item) for item in value["locations"]]
    if len({canonical(item) for item in locations}) != len(locations):
        raise ValueError("duplicate candidate location")
    return {"version": 1, "release_id": value["release_id"], "locations": locations}


def _cached(path, key, info):
    target = path / key
    if not target.exists() and not target.is_symlink():
        return False
    try:
        validate(local_get(path, key), info)
    except (ValueError, OSError) as error:
        raise CacheError("cache content needs explicit repair") from error
    return True


def _missing(content, cache, workspace):
    missing = workspace.table()
    for key, info in content.items():
        if not _cached(cache, key, info):
            missing[key] = info
    return missing


def _cache_complete(cache, rid):
    with closing(Workspace()) as workspace:
        if (
            not (cache / "releases" / rid).exists()
            and not (cache / "releases" / rid).is_symlink()
        ):
            return False
        try:
            _, content, _ = graph(
                rid, lambda key: local_get(cache, key), workspace=workspace
            )
        except FileNotFoundError:
            return False
        except (ValueError, OSError, KeyError, TypeError, AttributeError) as error:
            raise CacheError("cache metadata needs explicit repair") from error
        return all(_cached(cache, key, info) for key, info in content.items())


def _local_fetch(value, rid, cache, local_platform, limit):
    with closing(Workspace()) as workspace:
        source = filesystem_root(value, local_platform)

        def read(key):
            try:
                return local_get(source, key)
            except (ValueError, OSError) as error:
                raise CandidateError(
                    "filesystem store is unavailable or incomplete"
                ) from error

        try:
            _, content, _ = graph(rid, read, workspace=workspace)
        except (ValueError, KeyError, TypeError) as error:
            raise CandidateError(
                "filesystem store has invalid release metadata"
            ) from error
        missing = _missing(content, cache, workspace)
        if sum(info[0] for info in missing.values()) > limit:
            raise CandidateError("candidate exceeds the new-content byte budget")
        root_key = "releases/" + rid
        with tempfile.TemporaryDirectory(
            prefix=".prepare-", dir=cache.parent
        ) as temporary:
            staging = Path(temporary)
            for key, info in missing.items():
                data = read(key)
                try:
                    validate(data, info)
                except ValueError as error:
                    raise CandidateError(
                        "filesystem store has corrupt content"
                    ) from error
                target = staging / key
                target.parent.mkdir(parents=True, exist_ok=True)
                atomic_file(target, data)
            for key, info in content.items():
                if key == root_key:
                    continue
                if not _cached(cache, key, info):
                    data = local_get(staging, key)
                    validate(data, info)
                    target = cache / key
                    target.parent.mkdir(parents=True, exist_ok=True)
                    if not _cached(cache, key, info):
                        atomic_file(target, data)
            for key, info in content.items():
                if key != root_key:
                    validate(local_get(cache, key), info)
            if root_key in missing:
                data = local_get(staging, root_key)
                validate(data, content[root_key])
                target = cache / root_key
                target.parent.mkdir(parents=True, exist_ok=True)
                atomic_file(target, data)
        return {
            "physical_root": str(source),
            "downloaded_keys": len(missing),
            "downloaded_bytes": sum(info[0] for info in missing.values()),
            "cache_hits": len(content) - len(missing),
        }


def _s3_fetch(value, rid, cache, evidence, remotes, location_remotes, limit, options):
    uri = value["location_uri"]
    if remotes is None or location_remotes is None:
        raise CandidateError("S3 location needs an explicit access mapping")
    try:
        mapping = read_json(location_remotes)
        if not isinstance(mapping, dict) or not isinstance(mapping.get(uri), str):
            raise CandidateError("S3 location has no access mapping")
        config = remote_config(Aliases(remotes).list()[mapping[uri]])
    except (KeyError, ValueError, OSError) as error:
        raise CandidateError("S3 access profile is invalid") from error
    if uri != f"s3://{config['bucket']}/{config['prefix']}":
        raise CandidateError("S3 access profile does not match the exact store root")
    from .remote_cli import connection

    boundary = None
    try:
        options = dict(options)
        batch_size = options.pop("transfer_batch_size", 64)
        client, transport, path = connection(config, **options)
        boundary = Boundary(
            client,
            path,
            evidence,
            config["prefix"],
            bucket=config["bucket"],
            rclone_remote=config["rclone_remote"],
            transport=transport,
            batch_size=batch_size,
        )
        version = transport.version()
        boundary.repository()
        with closing(Workspace()) as workspace:
            _, content, _ = graph(rid, boundary.get, workspace=workspace)
            missing = _missing(content, cache, workspace)
            if sum(info[0] for info in missing.values()) > limit:
                raise CandidateError("candidate exceeds the new-content byte budget")
        result = boundary.fetch(rid, cache)
        return {**result, "access_alias": mapping[uri], "transport_version": version,
                "transfer_batch_size": batch_size}
    except CacheError:
        raise
    except (BotoCoreError, ClientError, ValueError) as error:
        raise CandidateError("S3 store could not supply verified content") from error
    finally:
        if boundary is not None:
            atomic_file(
                evidence / "transport.json",
                canonical(
                    {
                        "events": boundary.events,
                        "sdk_requests": boundary.requests,
                        "commands": boundary.adapter.commands,
                        "command_count": boundary.adapter.commands.count,
                        "transfer_count": boundary.transfers.count,
                        "commands_jsonl": "commands.jsonl",
                        "transfers_jsonl": "transfers.jsonl",
                        "evidence_preview_limit": 256,
                    }
                ),
            )


def prepare(
    value,
    cache,
    destination,
    evidence,
    *,
    local_platform=None,
    remotes=None,
    location_remotes=None,
    max_fetch_bytes=MAX_FETCH_BYTES,
    transfer_batch_size=64,
    **transport_options,
):
    """Return verified local paths and provenance for a submission adapter.

    The owner supplies a cooperative cache. Invalid cache bytes require explicit repair.
    Failed candidates never expose a checkout. No credentials are loaded for a cache hit.
    """
    if type(transfer_batch_size) is not int or not 1 <= transfer_batch_size <= 1024:
        raise ValueError("transfer batch size must be between 1 and 1024")
    transport_options["transfer_batch_size"] = transfer_batch_size
    value = reference(value)
    if local_platform is not None and local_platform not in LOCAL_PLATFORMS:
        raise ValueError("unsupported local platform")
    if type(max_fetch_bytes) is not int or max_fetch_bytes < 0:
        raise ValueError("new-content byte budget must be a nonnegative integer")
    destination = Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    cache, destination, evidence = map(
        lambda path: Path(path).resolve(), (cache, destination, evidence)
    )
    if (
        destination == cache
        or destination.is_relative_to(cache)
        or cache.is_relative_to(destination)
        or evidence == destination
        or evidence.is_relative_to(destination)
    ):
        raise ValueError("cache and evidence must be separate from the destination")
    if not Store(cache).fanout:
        raise ValueError("release preparation needs a fanout-2 cache")
    evidence.mkdir(parents=True, exist_ok=True)
    receipt_path = evidence / "prepare.receipt.json"
    if receipt_path.exists() or receipt_path.is_symlink():
        raise FileExistsError("use a new evidence directory for each preparation")
    rid, attempts, events = value["release_id"], [], []

    def timed(name, action):
        start = time.monotonic()
        result = action()
        events.append({"operation": name, "wall_s": time.monotonic() - start})
        return result

    try:
        complete = timed("cache-fresh-check", lambda: _cache_complete(cache, rid))
        selected = None
        fetched = {}
        if not complete:
            for number, candidate in enumerate(value["locations"]):
                start = time.monotonic()
                try:
                    if candidate["storage_platform"] == "s3":
                        fetched = _s3_fetch(
                            candidate,
                            rid,
                            cache,
                            evidence / f"attempt-{number:04}",
                            remotes,
                            location_remotes,
                            max_fetch_bytes,
                            transport_options,
                        )
                    else:
                        fetched = _local_fetch(
                            candidate, rid, cache, local_platform, max_fetch_bytes
                        )
                except CandidateError as error:
                    attempts.append(
                        {
                            "location": candidate,
                            "result": "refused",
                            "reason": str(error),
                            "wall_s": time.monotonic() - start,
                        }
                    )
                    continue
                if not _cache_complete(cache, rid):
                    raise CacheError("candidate left an incomplete cache")
                selected = candidate
                attempts.append(
                    {
                        "location": candidate,
                        "result": "verified",
                        "wall_s": time.monotonic() - start,
                    }
                )
                break
            else:
                raise ValueError("no candidate supplied a complete verified release")
        checkout = verified_checkout(cache, rid, destination, timed=timed)
        receipt = {
            "version": 1,
            "release_id": rid,
            "reference_blake3": digest(canonical(value)),
            "reference": value,
            "local_platform": local_platform,
            "max_fetch_bytes": max_fetch_bytes,
            "transfer_batch_size": transfer_batch_size,
            "cache_hit": complete,
            "selected_location": selected,
            "fetch": fetched,
            "cache": str(cache),
            "destination": str(destination),
            "checkout": checkout,
            "attempts": attempts,
            "events": events,
            "result": "verified",
            "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "resolver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        }
        atomic_file(receipt_path, canonical(receipt))
        return receipt
    finally:
        atomic_file(
            evidence / "resolution.json",
            canonical(
                {
                    "release_id": rid,
                    "reference_blake3": digest(canonical(value)),
                    "reference": value,
                    "local_platform": local_platform,
                    "max_fetch_bytes": max_fetch_bytes,
                    "attempts": attempts,
                    "events": events,
                }
            ),
        )
