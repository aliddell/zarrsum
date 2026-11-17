"""Bounded experimental remote commands. Credentials stay in private provider config."""

from __future__ import annotations

import argparse
import configparser
import json
import os
from pathlib import Path
import signal
import time

from .releases import atomic_file, canonical, identifier
from .remotes import Aliases, Boundary, REPOSITORY, remote_config, verified_checkout
from .transport import RcloneTransport


def connection(config, override=None, binary="rclone", bandwidth=None):
    import boto3
    from botocore.config import Config

    config = remote_config(config)
    path = str(override or config["rclone_config"])
    parsed = configparser.ConfigParser(interpolation=None)
    if not parsed.read(path) or config["rclone_remote"] not in parsed:
        raise ValueError("rclone configuration or remote is missing")
    remote = parsed[config["rclone_remote"]]
    if (
        remote.get("type") != "s3"
        or remote.get("endpoint", "").rstrip("/") != config["endpoint"]
        or remote.get("region") != config["region"]
        or remote.getboolean("env_auth", fallback=True)
        or remote.getboolean("force_path_style", fallback=False)
        != (config["addressing_style"] == "path")
        or not remote.get("access_key_id")
        or not remote.get("secret_access_key")
        or remote.getboolean("no_sign_request", fallback=False)
    ):
        raise ValueError("conflicting or incomplete SDK/rclone client settings")
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    session = boto3.Session(profile_name=config["profile"])
    client = session.client(
        "s3",
        endpoint_url=config["endpoint"],
        region_name=config["region"],
        config=Config(
            s3={"addressing_style": config["addressing_style"]},
            retries={"max_attempts": 0},
            connect_timeout=5,
            read_timeout=30,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    credentials = session.get_credentials().get_frozen_credentials()
    if (
        remote["access_key_id"] != credentials.access_key
        or remote["secret_access_key"] != credentials.secret_key
        or remote.get("session_token", "") != (credentials.token or "")
    ):
        raise ValueError("SDK profile and rclone credentials do not match")
    transport = RcloneTransport(binary=binary, config=path, bandwidth=bandwidth)
    return client, transport, path


def main(argv=None):
    parser = argparse.ArgumentParser(prog="zarrsum-release", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    remotes = sub.add_parser("remote")
    remotes.add_argument("--config", required=True)
    remote_sub = remotes.add_subparsers(dest="operation", required=True)
    add = remote_sub.add_parser("add")
    add.add_argument("name")
    for key in (
        "endpoint",
        "bucket",
        "prefix",
        "region",
        "profile",
        "rclone-remote",
        "rclone-config",
    ):
        add.add_argument("--" + key, required=True)
    add.add_argument("--addressing-style", choices=["path", "virtual"], default="path")
    add.add_argument("--format", default=REPOSITORY["format"])
    remote_sub.add_parser("list")
    remove = remote_sub.add_parser("remove")
    remove.add_argument("name")
    for command in ("push", "fetch", "pull", "checkout"):
        item = sub.add_parser(command)
        item.add_argument("store")
        item.add_argument("release_id")
        item.add_argument("--evidence", required=True)
        if command in ("checkout", "pull"):
            item.add_argument("destination")
        if command != "checkout":
            item.add_argument("--remote", required=True)
            item.add_argument("--remotes", required=True)
            item.add_argument("--rclone-binary", default="rclone")
            item.add_argument("--rclone-config")
            item.add_argument("--bwlimit")
    args = parser.parse_args(argv)
    evidence = None
    boundary = None
    output = {"result": "failed", "command": args.command}

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("transfer interrupted")

    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        if args.command == "remote":
            aliases = Aliases(args.config)
            if args.operation == "add":
                fields = (
                    "endpoint",
                    "bucket",
                    "prefix",
                    "region",
                    "profile",
                    "rclone_remote",
                    "rclone_config",
                    "addressing_style",
                    "format",
                )
                aliases.add(args.name, {key: getattr(args, key) for key in fields})
            elif args.operation == "remove":
                aliases.remove(args.name)
            output = aliases.list()
        else:
            identifier(args.release_id)
            evidence = Path(args.evidence)
            evidence.mkdir(parents=True, exist_ok=True)
            if args.command == "checkout":
                events = []

                def timed(name, action):
                    start = time.monotonic()
                    value = action()
                    events.append(
                        {"operation": name, "wall_s": time.monotonic() - start}
                    )
                    return value

                output = verified_checkout(
                    args.store, args.release_id, args.destination, timed=timed
                )
                output["events"] = events
                atomic_file(evidence / "checkout.receipt.json", canonical(output))
            else:
                if args.command == "pull" and (
                    Path(args.destination).exists()
                    or Path(args.destination).is_symlink()
                ):
                    raise FileExistsError(args.destination)
                config = remote_config(Aliases(args.remotes).list()[args.remote])
                client, transport, path = connection(
                    config, args.rclone_config, args.rclone_binary, args.bwlimit
                )
                boundary = Boundary(
                    client,
                    path,
                    evidence,
                    config["prefix"],
                    bucket=config["bucket"],
                    rclone_remote=config["rclone_remote"],
                    transport=transport,
                )
                version = transport.version()
                if args.command == "push":
                    output = boundary.push(args.store, args.release_id)
                else:
                    output = boundary.fetch(args.release_id, args.store)
                    if args.command == "pull":
                        output = {
                            "fetch": output,
                            "checkout": boundary.checkout(
                                args.store, args.release_id, args.destination
                            ),
                            "result": "verified",
                            "release_id": args.release_id,
                        }
                output["transport_version"] = version
        print(json.dumps(output, sort_keys=True))
        return 0
    except KeyboardInterrupt as error:
        output = {"result": "interrupted", "error": str(error)}
        parser.exit(130, "computed-release: interrupted\n")
    except Exception as error:
        output = {
            "result": "failed",
            "error": str(error),
            "error_type": type(error).__name__,
        }
        parser.exit(2, f"computed-release: {error}\n")
    finally:
        signal.signal(signal.SIGTERM, previous)
        if evidence is not None:
            if boundary is not None:
                output.update(
                    events=boundary.events,
                    transfers=boundary.transfers,
                    sdk_requests=boundary.requests,
                    commands=boundary.adapter.commands,
                )
            atomic_file(evidence / "result.json", canonical(output))
