"""Experimental local S3 boundary. The owner supplies private, cooperative stores.

This bounded protocol preserves computed-release/1 bytes. It does not supply remote
retention, authorization, scheduling, or garbage collection.
"""

from __future__ import annotations

from dataclasses import asdict
import json
import hashlib
import ipaddress
import configparser
from urllib.parse import urlsplit
import os
from pathlib import Path
import shutil
import tempfile
import time

from botocore.exceptions import ClientError
from .transport import RcloneTransport
from .releases import (
    Store,
    FORMAT,
    canonical,
    digest,
    member,
    identifier,
    descriptor,
    keys,
    partition_count,
    partition_of,
    atomic_file,
    sync_directory,
)

REPOSITORY = {
    "format": "computed-remote/experimental-1",
    "layout": "fanout-2",
    "roots": "releases/<blake3>",
    "manifests": "manifests/<blake3>",
    "registries": "registries/<blake3>",
    "objects": "objects/ab/cd/<blake3>",
}
LIMIT = 32 << 20
MAX_FILES = 10000


def checked(key):
    member(key)
    if "\n" in key or "\r" in key:
        raise ValueError("line break in key")
    return key


def object_key(value):
    identifier(value)
    return f"objects/{value[:2]}/{value[2:4]}/{value}"


def fixture_endpoint(endpoint, allowed_endpoints=None):
    """Accept only an explicit private fixture address and its selected port."""
    allowed = (
        {"http://127.0.0.1:13900"}
        if allowed_endpoints is None
        else set(allowed_endpoints)
    )
    parsed = urlsplit(endpoint)
    if (
        endpoint not in allowed
        or parsed.scheme != "http"
        or parsed.port != 13900
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise ValueError("endpoint is outside the isolated fixture allowlist")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError as error:
        raise ValueError("fixture endpoint needs a numeric private address") from error
    if (
        address.version != 4
        or not address.is_private
        or address.is_unspecified
        or address.is_multicast
    ):
        raise ValueError("fixture endpoint needs a private IPv4 address")
    return endpoint


def endpoint_url(value):
    parsed = urlsplit(value)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("invalid S3 endpoint")
    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        raise ValueError("invalid endpoint port")
    if parsed.scheme == "http":
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError as error:
            raise ValueError(
                "HTTP fixture requires a private numeric address"
            ) from error
        if not address.is_private or address.is_unspecified or address.is_multicast:
            raise ValueError("HTTP fixture requires a private address")
    return value.rstrip("/")


def remote_config(value):
    import re

    fields = {
        "endpoint",
        "bucket",
        "prefix",
        "region",
        "addressing_style",
        "profile",
        "rclone_remote",
        "rclone_config",
        "format",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("invalid remote configuration fields")
    config = dict(value)
    config["endpoint"] = endpoint_url(config["endpoint"])
    if config["format"] != REPOSITORY["format"]:
        raise ValueError("unsupported repository format")
    checked(config["prefix"])
    if (
        not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", config["bucket"])
        or ".." in config["bucket"]
    ):
        raise ValueError("invalid bucket")
    for field in ("region", "profile", "rclone_remote"):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", config[field]):
            raise ValueError("invalid profile, region, or rclone name")
    if (
        config["addressing_style"] not in ("path", "virtual")
        or not config["rclone_config"]
    ):
        raise ValueError("invalid addressing mode or rclone config reference")
    return config


class Aliases:
    """Store non-secret remote references. Removal makes no remote requests."""

    def __init__(self, path, *, allowed_endpoints=None):
        self.path = Path(path)
        self.allowed_endpoints = allowed_endpoints

    def list(self):
        return json.loads(self.path.read_bytes()) if self.path.exists() else {}

    def add(self, name, config):
        checked(name)
        config = remote_config(config)
        if self.allowed_endpoints is not None:
            fixture_endpoint(config["endpoint"], self.allowed_endpoints)
        found = self.list()
        if name in found:
            raise ValueError("alias exists")
        found[name] = config
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_file(self.path, canonical(found))

    def remove(self, name):
        found = self.list()
        del found[name]
        atomic_file(self.path, canonical(found))


def graph(release_id, get):
    """Validate each metadata digest before following any of its references."""
    identifier(release_id)
    content = {}

    def metadata(kind, value):
        key = kind + "/" + identifier(value)
        data = get(key)
        if len(data) > LIMIT or digest(data) != value:
            raise ValueError("metadata bytes mismatch: " + key)
        content[key] = [len(data), value]
        return data

    root = json.loads(metadata("releases", release_id))
    if set(root) != {"format", "descriptor", "partitions"} or root["format"] != FORMAT:
        raise ValueError("invalid release root")
    registries = {}
    for family in root["descriptor"].get("families", []):
        value = identifier(family["registry"])
        registries[value] = metadata("registries", value)
    spec = descriptor(root["descriptor"], registries)
    expected = set()
    for key in keys(spec, registries):
        checked(key)
        if key in expected or len(expected) >= MAX_FILES:
            raise ValueError("duplicate or oversized graph")
        expected.add(key)
    records = {}
    partitions = set()
    if len(root["partitions"]) > partition_count(spec):
        raise ValueError("too many partitions")
    for entry in root["partitions"]:
        if not isinstance(entry, list) or len(entry) != 4:
            raise ValueError("invalid partition")
        part, value, count, size = entry
        if (
            type(part) is not int
            or not 0 <= part < partition_count(spec)
            or part in partitions
            or type(count) is not int
            or count < 1
            or type(size) is not int
            or size < 0
        ):
            raise ValueError("invalid partition values")
        partitions.add(part)
        rows = json.loads(metadata("manifests", value))
        if not isinstance(rows, list) or len(rows) != count:
            raise ValueError("partition count mismatch")
        total = 0
        for row in rows:
            if not isinstance(row, list) or len(row) != 3:
                raise ValueError("invalid file record")
            key, length, value = row
            checked(key)
            identifier(value)
            if (
                key not in expected
                or key in records
                or partition_of(key, partition_count(spec)) != part
                or type(length) is not int
                or not 0 <= length <= LIMIT
            ):
                raise ValueError("invalid file membership or length")
            records[key] = row
            total += length
            obj = object_key(value)
            if obj in content and content[obj] != [length, value]:
                raise ValueError("inconsistent object length")
            content[obj] = [length, value]
        if total != size:
            raise ValueError("partition size mismatch")
    if set(records) != expected:
        raise ValueError("incomplete graph")
    return root, content, records


def local_get(directory, key):
    path = Path(directory) / checked(key)
    if path.is_symlink() or not path.resolve().is_relative_to(
        Path(directory).resolve()
    ):
        raise ValueError("unsafe local store path")
    with path.open("rb") as stream:
        data = stream.read(LIMIT + 1)
    if len(data) > LIMIT:
        raise ValueError("object too large")
    return data


def validate(data, info):
    if len(data) != info[0] or digest(data) != info[1]:
        raise ValueError("content bytes mismatch")


class Boundary:
    def __init__(
        self,
        client,
        rconfig,
        evidence,
        prefix="retained",
        *,
        allowed_endpoints=None,
        bucket="release-test",
        rclone_remote="s3",
        binary="rclone",
        transport=None,
    ):
        endpoint = endpoint_url(client.meta.endpoint_url)
        if allowed_endpoints is not None:
            fixture_endpoint(endpoint, allowed_endpoints)
            config = configparser.ConfigParser(interpolation=None)
            config.read(rconfig)
            remote = config[rclone_remote]
            if (
                remote.get("endpoint") != endpoint
                or remote.get("region") != "garage"
                or remote.getboolean("env_auth", fallback=True)
                or not remote.getboolean("force_path_style", fallback=False)
            ):
                raise ValueError(
                    "rclone configuration does not match the allowed SDK endpoint"
                )
        self.client, self.bucket = client, bucket
        self.rclone_remote = rclone_remote
        self.prefix = checked(prefix)
        self.evidence = Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.adapter = transport or RcloneTransport(binary=binary, config=rconfig)
        self.events, self.transfers = [], []
        self.requests = {}

        def count(model, **_):
            name = model.name
            self.requests[name] = self.requests.get(name, 0) + 1

        client.meta.events.register("before-call.s3", count)

    def receipt(self, name, value):
        from . import transport

        sources = {
            "remotes": Path(__file__),
            "releases": Path(__file__).with_name("releases.py"),
            "direct_rclone": Path(transport.__file__),
        }
        value.update(
            utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            repository={
                "endpoint": self.client.meta.endpoint_url,
                "bucket": self.bucket,
                "prefix": self.prefix,
                "format": REPOSITORY["format"],
            },
            code_sha256={
                name: hashlib.sha256(path.read_bytes()).hexdigest()
                for name, path in sources.items()
            },
        )
        atomic_file(self.evidence / (name + ".receipt.json"), canonical(value))

    def timed(self, operation, function):
        start = time.monotonic()
        value = function()
        self.events.append(
            {
                "operation": operation,
                "wall_s": time.monotonic() - start,
                "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        )
        return value

    def get(self, key):
        response = self.client.get_object(
            Bucket=self.bucket, Key=self.prefix + "/" + checked(key)
        )
        with response["Body"] as stream:
            data = stream.read(LIMIT + 1)
        if len(data) > LIMIT:
            raise ValueError("remote object too large")
        return data

    def maybe(self, key):
        try:
            return self.get(key)
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            raise

    def put(self, key, data):
        return self.client.put_object(
            Bucket=self.bucket, Key=self.prefix + "/" + checked(key), Body=data
        )

    def conditional(self, key, data):
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self.prefix + "/" + checked(key),
                Body=data,
                IfNoneMatch="*",
            )
            return "created"
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                raise
            if self.get(key) != data:
                raise ValueError("conditional root conflict")
            return "equal-existing"

    def repository(self, create=False):
        raw = canonical(REPOSITORY)
        if create:
            self.conditional("repository.json", raw)
        if self.get("repository.json") != raw:
            raise ValueError("unsupported repository format")

    def plan(self, release_id, source, destination, entries, name):
        if len(entries) > MAX_FILES:
            raise ValueError("plan too large")
        data = canonical(
            {
                "version": 1,
                "release_id": identifier(release_id),
                "source": source,
                "destination": destination,
                "entries": sorted(entries),
            }
        )
        path = self.evidence / (name + ".plan.json")
        atomic_file(path, data)
        pinned = digest(data)
        atomic_file(
            self.evidence / (name + ".submission.json"),
            canonical({"plan": path.name, "blake3": pinned}),
        )
        return path, pinned

    def execute(self, path, pinned, interrupt_after=None):
        raw = path.read_bytes()
        if digest(raw) != pinned:
            raise ValueError("transfer plan changed")
        plan = json.loads(raw)
        entries = plan["entries"]
        for offset in range(0, len(entries), 64):
            if digest(path.read_bytes()) != pinned:
                raise ValueError("transfer plan changed before retry")
            batch = [checked(e[0]) for e in entries[offset : offset + 64]]
            result = self.adapter.copy(plan["source"], plan["destination"], batch)
            self.transfers.append(
                {
                    "plan_blake3": pinned,
                    "offset": offset,
                    "keys": len(batch),
                    "result": asdict(result),
                }
            )
            if not result.ok:
                raise ValueError("rclone file transfer failed: " + result.detail)
            if interrupt_after is not None and offset // 64 + 1 >= interrupt_after:
                raise InterruptedError(
                    "injected coordinator interruption after completed batch"
                )

    def push(self, sender, release_id, name="push", interrupt_after=None):
        sender = Path(sender).resolve(strict=True)
        self.repository(create=True)
        root, content, records = self.timed(
            "source-metadata", lambda: graph(release_id, lambda k: local_get(sender, k))
        )
        root_key = "releases/" + release_id

        def source_check():
            for key, info in content.items():
                validate(local_get(sender, key), info)

        self.timed("source-fresh-check", source_check)
        missing, reused = [], 0

        def inspect():
            nonlocal reused
            for key, info in content.items():
                if key == root_key:
                    continue
                existing = self.maybe(key)
                if existing is None:
                    missing.append([key, *info])
                else:
                    validate(existing, info)
                    reused += 1

        self.timed("target-reuse-readback", inspect)
        plan, pin = self.plan(
            release_id,
            str(sender),
            f"{self.rclone_remote}:{self.bucket}/{self.prefix}",
            missing,
            name,
        )
        self.timed("upload", lambda: self.execute(plan, pin, interrupt_after))

        def target_check():
            for key, info in content.items():
                if key != root_key:
                    validate(self.get(key), info)

        self.timed("target-fresh-readback", target_check)
        status = self.timed(
            "publish-root-last",
            lambda: self.conditional(root_key, local_get(sender, root_key)),
        )
        receipt = {
            "release_id": release_id,
            "coverage": "full",
            "method": "fresh-BLAKE3-target-readback",
            "result": "verified",
            "root": status,
            "uploaded_keys": len(missing),
            "reused_keys": reused,
            "uploaded_bytes": sum(e[1] for e in missing),
            "graph_keys": len(content),
            "graph_bytes": sum(i[0] for i in content.values()),
            "files": len(records),
            "bytes": sum(r[1] for r in records.values()),
            "plan_blake3": pin,
        }
        self.receipt(name, receipt)
        return receipt

    def fetch(self, release_id, receiver, name="fetch"):
        receiver = Path(receiver)
        self.repository()
        metadata = {}

        def read(key):
            data = self.get(key)
            metadata[key] = data
            return data

        root, content, records = self.timed(
            "metadata-resolution", lambda: graph(release_id, read)
        )
        store = Store(receiver)
        missing = []
        hits = 0

        def inspect():
            nonlocal hits
            for key, info in content.items():
                if key in metadata:
                    continue
                path = receiver / key
                if path.exists() or path.is_symlink():
                    validate(local_get(receiver, key), info)
                    hits += 1
                else:
                    missing.append([key, *info])

        self.timed("local-cache-readback", inspect)
        incoming = Path(tempfile.mkdtemp(prefix=".incoming-", dir=receiver.parent))
        try:
            plan, pin = self.plan(
                release_id,
                f"{self.rclone_remote}:{self.bucket}/{self.prefix}",
                str(incoming),
                missing,
                name,
            )
            self.timed("download", lambda: self.execute(plan, pin))

            def admit():
                for key, info in content.items():
                    if key.startswith("releases/"):
                        continue
                    path = receiver / key
                    data = metadata.get(key)
                    if data is None:
                        data = local_get(receiver if path.exists() else incoming, key)
                    validate(data, info)
                    if path.exists():
                        validate(local_get(receiver, key), info)
                    else:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        atomic_file(path, data)
                for key, info in content.items():
                    if not key.startswith("releases/"):
                        validate(local_get(receiver, key), info)
                atomic_file(
                    receiver / ("releases/" + release_id),
                    metadata["releases/" + release_id],
                )
                store.verify(release_id, workers=4)

            self.timed("local-admission-and-fresh-check", admit)
        finally:
            shutil.rmtree(incoming)
        result = {
            "release_id": release_id,
            "coverage": "full",
            "method": "fresh-BLAKE3-local-admission",
            "result": "verified",
            "downloaded_objects": len(missing),
            "cache_hits": hits,
            "downloaded_bytes": sum(e[1] for e in missing),
            "metadata_bytes": sum(map(len, metadata.values())),
        }
        self.receipt(name, result)
        return result

    def checkout(self, receiver, release_id, destination, name="checkout"):
        result = verified_checkout(receiver, release_id, destination, timed=self.timed)
        self.receipt(name, result)
        return result


def verified_checkout(receiver, release_id, destination, *, timed=None):
    if timed is None:
        timed = lambda _name, action: action()
    identifier(release_id)
    destination = Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    store = Store(receiver)
    graph(release_id, lambda key: local_get(store.path, key))
    private = Path(tempfile.mkdtemp(prefix=".checkout-", dir=destination.parent))
    staging = private / "tree"
    try:
        timed("checkout-create", lambda: store.restore(release_id, staging, workers=4))
        verdict = timed(
            "destination-fresh-verify",
            lambda: store.verify(release_id, staging, workers=4),
        )
        result = {
            **verdict,
            "coverage": "full",
            "method": "fresh-BLAKE3-destination-readback",
            "result": "verified",
            "verifier": "direct-rclone-experimental",
        }
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(destination)
        os.rename(staging, destination)
        sync_directory(destination.parent)
        return result
    finally:
        shutil.rmtree(private)
