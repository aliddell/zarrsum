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
        "<name>": {"path", "unit", "subpath", "policy", "format", "id", "n_stores",
                   "n_arrays", "n_files"}
      }
    }

``unit`` records what the entry pins (the whole dataset ``store``, one ``group``/site, or one
``array``). For a ``group`` or ``array`` the entry also keeps ``subpath`` -- the path of that
node relative to the dataset root -- and ``id`` is that node's zarrsum id, taken from the
``groups``/``arrays`` lists ``hash_dataset`` returns. ``verify`` recomputes and compares the
id of the same node, so a subset pin is verified against the subset, not the whole dataset.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .core import hash_dataset, DatasetMetadataError

MANIFEST_VERSION = 1


def _select(r: dict, unit: str, subpath: str | None) -> dict:
    """Pick the pinned id and its counts from a ``hash_dataset`` result, by ``unit``.

    ``store`` is the whole-dataset id. ``group``/``array`` need ``subpath``, the node path
    relative to the dataset root (as printed by ``zarrsum --groups``/``--arrays``); the id is
    that node's id. Raises ``KeyError`` if the node is not found."""
    if unit == "store":
        return {"id": r["dataset_id"], "n_stores": r["n_stores"],
                "n_arrays": r["n_arrays"], "n_files": r["n_files"]}
    if subpath is None:
        raise ValueError(f"unit {unit!r} needs a subpath (the node path relative to the root)")
    if unit == "group":
        for rel, i in r["groups"]:
            if rel == subpath:
                return {"id": i}
        raise KeyError(f"no group at {subpath!r}; groups are {[g[0] for g in r['groups']]}")
    if unit == "array":
        for rel, i, n in r["arrays"]:
            if rel == subpath:
                return {"id": i, "n_files": n}
        raise KeyError(f"no array at {subpath!r}; arrays are {[a[0] for a in r['arrays']]}")
    raise ValueError(f"unit must be store, group, or array, not {unit!r}")


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
                unit: str = "store", subpath: str | None = None, max_workers=None,
                fence=False, min_quiet_s=None) -> dict:
    """Hash ``dataset_path`` and record it in the manifest under ``name``. ``unit`` is
    ``store`` (whole dataset), ``group``, or ``array``; a group/array pin needs ``subpath``,
    the node path relative to the dataset root. ``fence``/``min_quiet_s`` pass through to
    ``hash_dataset`` as mid-write guards."""
    r = hash_dataset(dataset_path, max_workers=max_workers, policy=policy, fence=fence,
                     min_quiet_s=min_quiet_s)
    sel = _select(r, unit, subpath)
    m = load(manifest_path)
    entry = {
        "path": dataset_path, "unit": unit, "subpath": subpath, "policy": policy,
        "format": r["format"], "id": sel["id"],
        "n_stores": sel.get("n_stores"), "n_arrays": sel.get("n_arrays"),
        "n_files": sel.get("n_files"),
    }
    m["datasets"][name] = entry
    save(manifest_path, m)
    return entry


def verify_dataset(manifest_path: str, name: str, dataset_path: str | None = None,
                   max_workers=None, fence=False, min_quiet_s=None) -> dict:
    """Recompute the id of the pinned node and compare it to its manifest pin. For a group or
    array pin, the comparison targets that same node, not the whole dataset."""
    m = load(manifest_path)
    if name not in m.get("datasets", {}):
        raise KeyError(f"no dataset named {name!r} in {manifest_path}")
    entry = m["datasets"][name]
    path = dataset_path or entry["path"]
    r = hash_dataset(path, max_workers=max_workers, policy=entry["policy"], fence=fence,
                     min_quiet_s=min_quiet_s)
    sel = _select(r, entry.get("unit", "store"), entry.get("subpath"))
    return {
        "ok": sel["id"] == entry["id"] and r["format"] == entry["format"],
        "name": name, "path": path, "policy": entry["policy"],
        "unit": entry.get("unit", "store"), "subpath": entry.get("subpath"),
        "expected": entry["id"], "actual": sel["id"],
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="zarrsum-manifest",
                                 description="Pin and verify datasets by zarrsum id.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pa = sub.add_parser("add", help="hash a dataset and record it in the manifest")
    pa.add_argument("manifest"); pa.add_argument("name"); pa.add_argument("dataset_path")
    pa.add_argument("--policy", choices=["bytes", "inventory"], default="inventory")
    pa.add_argument("--unit", choices=["store", "group", "array"], default="store")
    pa.add_argument("--path", default=None, metavar="RELPATH",
                    help="for --unit group/array: the node path relative to the dataset root "
                         "(as printed by `zarrsum --groups`/`--arrays`)")
    pa.add_argument("--workers", type=int, default=None)
    pa.add_argument("--fence", action="store_true", help="abort if a file changes mid-hash")
    pa.add_argument("--min-quiet", type=float, default=None, metavar="SECONDS",
                    help="refuse unless the newest file mtime is older than SECONDS")

    pv = sub.add_parser("verify", help="recompute the pinned node's id and compare to its pin")
    pv.add_argument("manifest"); pv.add_argument("name")
    pv.add_argument("dataset_path", nargs="?", default=None)
    pv.add_argument("--workers", type=int, default=None)
    pv.add_argument("--fence", action="store_true", help="abort if a file changes mid-hash")
    pv.add_argument("--min-quiet", type=float, default=None, metavar="SECONDS",
                    help="refuse unless the newest file mtime is older than SECONDS")

    a = ap.parse_args(argv)
    if a.cmd == "add":
        try:
            e = add_dataset(a.manifest, a.name, a.dataset_path, policy=a.policy, unit=a.unit,
                            subpath=a.path, max_workers=a.workers, fence=a.fence,
                            min_quiet_s=a.min_quiet)
        except (KeyError, ValueError, DatasetMetadataError) as e:
            print(f"zarrsum-manifest: {e}", file=sys.stderr)
            return 2
        where = f"{e['unit']}:{e['subpath']}" if e["subpath"] else e["unit"]
        print(f"pinned {a.name}: {e['id']}  ({e['format']}, unit={where}, "
              f"files={e['n_files']})")
        return 0
    try:
        r = verify_dataset(a.manifest, a.name, a.dataset_path, max_workers=a.workers,
                           fence=a.fence, min_quiet_s=a.min_quiet)
    except (KeyError, DatasetMetadataError) as e:
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
