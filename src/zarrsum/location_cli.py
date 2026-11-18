"""Prepare pinned inputs without changing the training data loader."""

from __future__ import annotations

import argparse
import json
import signal
from pathlib import Path

from .locations import MAX_FETCH_BYTES, prepare, read_json


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="zarrsum-release prepare", description=__doc__
    )
    parser.add_argument("reference")
    parser.add_argument("cache")
    parser.add_argument("destination")
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--local-platform", choices=["reef", "sf_hpc"])
    parser.add_argument("--remotes")
    parser.add_argument("--location-remotes")
    parser.add_argument("--rclone-binary", default="rclone")
    parser.add_argument("--transfer-timeout", type=float, default=3600)
    parser.add_argument("--max-fetch-bytes", type=int, default=MAX_FETCH_BYTES)
    args = parser.parse_args(argv)
    import math

    if (
        not math.isfinite(args.transfer_timeout)
        or not 0 < args.transfer_timeout <= 3600
    ):
        parser.error("transfer timeout must be finite and between 0 and 3600 seconds")

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("preparation interrupted")

    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        result = prepare(
            read_json(args.reference),
            args.cache,
            args.destination,
            args.evidence,
            local_platform=args.local_platform,
            remotes=args.remotes,
            location_remotes=args.location_remotes,
            max_fetch_bytes=args.max_fetch_bytes,
            binary=args.rclone_binary,
            timeout=args.transfer_timeout,
        )
    except KeyboardInterrupt:
        parser.exit(130, "computed-release: preparation interrupted\n")
    except (ValueError, OSError) as error:
        parser.exit(2, f"computed-release: {error}\n")
    finally:
        signal.signal(signal.SIGTERM, previous)
    print(
        json.dumps(
            {
                "release_id": result["release_id"],
                "destination": result["destination"],
                "receipt": str(Path(args.evidence).resolve() / "prepare.receipt.json"),
                "result": result["result"],
            },
            sort_keys=True,
        )
    )
    return 0
