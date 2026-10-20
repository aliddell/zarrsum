"""Command line for zarrsum: print the content id of a Zarr dataset."""
from __future__ import annotations

import argparse
import json
import sys
import time

from .core import hash_dataset, DatasetChangedError
from . import __version__


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="zarrsum",
        description="Compute a Zarr-aware content id (BLAKE3 Merkle tree).",
    )
    ap.add_argument("path", help="a Zarr store, or a directory of stores and other files")
    ap.add_argument("--policy", choices=["bytes", "inventory"], default="bytes",
                    help="leaf policy: bytes (default, read every byte) or inventory "
                         "(data-file size only, no byte read)")
    ap.add_argument("--arrays", action="store_true", help="also print per-array ids")
    ap.add_argument("--groups", action="store_true", help="also print per-group ids")
    ap.add_argument("--json", action="store_true", help="print the full result as JSON")
    ap.add_argument("--workers", type=int, default=None,
                    help="file-hashing threads (default: auto; 1 = serial)")
    ap.add_argument("--ignore", action="append", default=None, metavar="GLOB",
                    help="directory-name glob to skip (repeatable; replaces the default set)")
    ap.add_argument("--cache", default=None, metavar="PATH",
                    help="reuse cache (bytes policy): re-read only changed files")
    ap.add_argument("--verify", action="store_true",
                    help="ignore the reuse cache (re-read everything) but refresh it")
    ap.add_argument("--version", action="version", version=f"zarrsum {__version__}")
    a = ap.parse_args(argv)

    t0 = time.time()
    try:
        r = hash_dataset(a.path, max_workers=a.workers, policy=a.policy,
                         ignore=a.ignore, cache=a.cache, verify=a.verify)
    except DatasetChangedError as e:
        print(f"zarrsum: {e}", file=sys.stderr)
        return 2
    wall = round(time.time() - t0, 2)

    if a.json:
        out = dict(r)
        out["wall_s"] = wall
        if not a.arrays:
            out.pop("arrays", None)
        if not a.groups:
            out.pop("groups", None)
        print(json.dumps(out, indent=2))
        return 0

    print(f"dataset_id {r['dataset_id']}  ({r['format']})")
    print(
        f"stores={r['n_stores']} groups={r['n_groups']} arrays={r['n_arrays']} "
        f"files={r['n_files']} nonzarr={r['n_nonzarr']} wall={wall}s"
    )
    for rel, i in r["stores"]:
        print(f"  store {rel}  {i}")
    if a.groups:
        for rel, i in r["groups"]:
            print(f"  group {rel}  {i}")
    if a.arrays:
        for rel, i, n in r["arrays"]:
            print(f"  array {rel}  files={n}  {i}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
