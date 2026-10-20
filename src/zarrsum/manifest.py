"""A tiny training manifest that pins datasets by their zarrsum id.

The manifest is a small JSON file kept in git. It records, for each dataset, the zarrsum id
and the policy that made it. A storage tool (DVC) moves the bytes; this manifest records the
identity, so a consumer can **verify** that the bytes it fetched are the exact dataset the
manifest names. That verification -- a portable, Zarr-aware check -- is what the manifest
adds on top of DVC.

Format (JSON):

    {
      "manifest_version": 1,
      "datasets": {
        "<name>": {"path", "unit", "policy", "format", "id", "n_stores", "n_arrays", "n_files"}
      }
    }

``unit`` records what the entry pins (whole ``store``, a ``group``/site, or an ``array``);
the id is the matching zarrsum id. The CLI pins the whole-dataset id; pinning a subset id is
a library call (pass the group/array id from ``hash_dataset``).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .core import hash_dataset

MANIFEST_VERSION = 1


def load(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"manifest_version": MANIFEST_VERSION, "datasets": {}}


def save(path: str, m: dict) -> None:
    with open(path, "w") as f:
        json.dump(m, f, indent=2, sort_keys=True)
        f.write("\n")


def add_dataset(manifest_path: str, name: str, dataset_path: str, policy: str = "inventory",
                unit: str = "store", max_workers=None) -> dict:
    """Hash ``dataset_path`` and record it in the manifest under ``name``."""
    r = hash_dataset(dataset_path, max_workers=max_workers, policy=policy)
    m = load(manifest_path)
    entry = {
        "path": dataset_path, "unit": unit, "policy": policy, "format": r["format"],
        "id": r["dataset_id"], "n_stores": r["n_stores"], "n_arrays": r["n_arrays"],
        "n_files": r["n_files"],
    }
    m["datasets"][name] = entry
    save(manifest_path, m)
    return entry


def verify_dataset(manifest_path: str, name: str, dataset_path: str | None = None,
                   max_workers=None) -> dict:
    """Recompute the id of a dataset and compare it to its manifest pin."""
    m = load(manifest_path)
    if name not in m.get("datasets", {}):
        raise KeyError(f"no dataset named {name!r} in {manifest_path}")
    entry = m["datasets"][name]
    path = dataset_path or entry["path"]
    r = hash_dataset(path, max_workers=max_workers, policy=entry["policy"])
    return {
        "ok": r["dataset_id"] == entry["id"] and r["format"] == entry["format"],
        "name": name, "path": path, "policy": entry["policy"],
        "expected": entry["id"], "actual": r["dataset_id"],
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="zarrsum-manifest",
                                 description="Pin and verify datasets by zarrsum id.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pa = sub.add_parser("add", help="hash a dataset and record it in the manifest")
    pa.add_argument("manifest"); pa.add_argument("name"); pa.add_argument("dataset_path")
    pa.add_argument("--policy", choices=["bytes", "inventory"], default="inventory")
    pa.add_argument("--unit", choices=["store", "group", "array"], default="store")
    pa.add_argument("--workers", type=int, default=None)

    pv = sub.add_parser("verify", help="recompute a dataset's id and compare to its pin")
    pv.add_argument("manifest"); pv.add_argument("name")
    pv.add_argument("dataset_path", nargs="?", default=None)
    pv.add_argument("--workers", type=int, default=None)

    a = ap.parse_args(argv)
    if a.cmd == "add":
        e = add_dataset(a.manifest, a.name, a.dataset_path, policy=a.policy, unit=a.unit,
                        max_workers=a.workers)
        print(f"pinned {a.name}: {e['id']}  ({e['format']}, unit={e['unit']}, "
              f"files={e['n_files']})")
        return 0
    try:
        r = verify_dataset(a.manifest, a.name, a.dataset_path, max_workers=a.workers)
    except KeyError as e:
        print(f"zarrsum-manifest: {e}", file=sys.stderr)
        return 2
    if r["ok"]:
        print(f"OK {r['name']}: matches pin {r['expected']}  ({r['policy']})")
        return 0
    print(f"MISMATCH {r['name']} ({r['policy']})\n  expected {r['expected']}\n  actual   {r['actual']}",
          file=sys.stderr)
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
