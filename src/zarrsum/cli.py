"""Command line for zarrsum: print the content id of a Zarr dataset."""
from __future__ import annotations

import argparse
import json
import time

from .core import hash_dataset
from . import __version__


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="zarrsum",
        description="Compute a Zarr-aware content id (BLAKE3 Merkle tree).",
    )
    ap.add_argument("path", help="a Zarr store, or a directory of stores and other files")
    ap.add_argument("--arrays", action="store_true", help="also print per-array ids")
    ap.add_argument("--json", action="store_true", help="print the full result as JSON")
    ap.add_argument("--version", action="version", version=f"zarrsum {__version__}")
    a = ap.parse_args(argv)

    t0 = time.time()
    r = hash_dataset(a.path)
    wall = round(time.time() - t0, 2)

    if a.json:
        out = dict(r)
        out["wall_s"] = wall
        if not a.arrays:
            out.pop("arrays", None)
        print(json.dumps(out, indent=2))
        return 0

    print(f"dataset_id {r['dataset_id']}")
    print(
        f"stores={r['n_stores']} arrays={r['n_arrays']} chunks={r['n_chunks']} "
        f"nonzarr={r['n_nonzarr']} wall={wall}s"
    )
    for rel, i in r["stores"]:
        print(f"  store {rel}  {i}")
    if a.arrays:
        for rel, i, n in r["arrays"]:
            print(f"  array {rel}  chunks={n}  {i}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
