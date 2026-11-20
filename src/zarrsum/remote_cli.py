"""Bounded experimental remote commands. Credentials stay in private provider config."""

from __future__ import annotations

import argparse
import configparser
import json
import os
from pathlib import Path
import signal
import sys
import time

from .releases import atomic_file, canonical, identifier
from .remotes import Aliases, Boundary, REPOSITORY, remote_config, verified_checkout
from .transport import RcloneTransport


def connection(config, override=None, binary="rclone", bandwidth=None, timeout=3600):
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
    transport = RcloneTransport(
        binary=binary, config=path, bandwidth=bandwidth, timeout=timeout
    )
    return client, transport, path


def main(argv=None):
    def timeout_seconds(value):
        import math

        seconds = float(value)
        if not math.isfinite(seconds) or not 0 < seconds <= 3600:
            raise argparse.ArgumentTypeError(
                "transfer timeout must be finite and between 0 and 3600 seconds"
            )
        return seconds

    def batch_keys(value):
        number = int(value)
        if not 1 <= number <= 1024:
            raise argparse.ArgumentTypeError(
                "transfer batch size must be between 1 and 1024"
            )
        return number

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
        if command == "pull":
            pull = item
            item.add_argument("store", nargs="?", help="local store for a directory pull")
            item.add_argument(
                "--ozx", action="store_true", help="write one OZX without a local store"
            )
            item.add_argument(
                "--workers", type=int, choices=range(1, 9),
                help="OZX download workers (default: 4)",
            )
        else:
            item.add_argument("store")
        item.add_argument("release_id")
        item.add_argument("--evidence", required=True)
        if command == "push":
            item.add_argument("--resume-plan")
            item.add_argument("--plan-digest")
        if command in ("checkout", "pull"):
            item.add_argument("destination")
        if command != "checkout":
            item.add_argument("--remote", required=True)
            item.add_argument("--remotes", required=True)
            item.add_argument("--rclone-binary", default="rclone")
            item.add_argument("--rclone-config")
            item.add_argument("--bwlimit")
            item.add_argument("--transfer-timeout", type=timeout_seconds, default=3600)
            item.add_argument("--transfer-batch-size", type=batch_keys, default=64)
    supplied = sys.argv[1:] if argv is None else argv
    if supplied and supplied[0] == "pull":
        args = pull.parse_intermixed_args(
            supplied[1:], namespace=argparse.Namespace(command="pull")
        )
    else:
        args = parser.parse_args(supplied)
    direct_ozx = args.command == "pull" and args.ozx
    if args.command == "pull":
        if direct_ozx and args.store is not None:
            parser.error("pull --ozx takes RELEASE_ID DESTINATION without STORE")
        if not direct_ozx and args.store is None:
            parser.error("pull requires STORE RELEASE_ID DESTINATION unless --ozx is set")
        if not direct_ozx and args.workers is not None:
            parser.error("pull --workers requires --ozx")
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
            if args.command == "push":
                if bool(args.resume_plan) != bool(args.plan_digest):
                    raise ValueError(
                        "resume needs both --resume-plan and --plan-digest"
                    )
                if args.plan_digest:
                    identifier(args.plan_digest)
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
                    config,
                    args.rclone_config,
                    args.rclone_binary,
                    args.bwlimit,
                    args.transfer_timeout,
                )
                boundary = Boundary(
                    client,
                    path,
                    evidence,
                    config["prefix"],
                    bucket=config["bucket"],
                    rclone_remote=config["rclone_remote"],
                    transport=transport,
                    batch_size=args.transfer_batch_size,
                )
                if not direct_ozx:
                    version = transport.version()
                if direct_ozx:
                    from .ozx import write_ozx

                    boundary.repository()
                    output = boundary.timed(
                        "write-ozx",
                        lambda: write_ozx(
                            args.release_id,
                            boundary.get,
                            args.destination,
                            workers=args.workers or 4,
                        ),
                    )
                    boundary.receipt("pull-ozx", output)
                elif args.command == "push":
                    output = boundary.push(
                        args.store,
                        args.release_id,
                        resume_plan=args.resume_plan,
                        plan_digest=args.plan_digest,
                    )
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
                if not direct_ozx:
                    output["transport_version"] = version
                    output["transfer_batch_size"] = args.transfer_batch_size
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
                    command_count=getattr(
                        boundary.adapter.commands,
                        "count",
                        len(boundary.adapter.commands),
                    ),
                    transfer_count=boundary.transfers.count,
                    commands_jsonl="commands.jsonl",
                    transfers_jsonl="transfers.jsonl",
                    evidence_preview_limit=256,
                )
            atomic_file(evidence / "result.json", canonical(output))
