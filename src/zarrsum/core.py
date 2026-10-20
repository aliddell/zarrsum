"""Core of the Zarr content id.

The id is a BLAKE3 Merkle tree over the data files on disk and the canonical Zarr
metadata: file -> array -> group -> store, then a dataset id that combines the store ids
with the non-Zarr files. A leaf is one data file: one chunk for an unsharded array, one
shard for a sharded Zarr v3 array (the shard is not opened).

Two leaf policies:

- ``bytes`` (default): a data-file leaf is ``BLAKE3(file bytes)``. Always sound. Reads
  every byte, so the first pass is read-bound.
- ``inventory``: a data-file leaf is ``BLAKE3(file size)`` -- a stat, no byte read. Near
  free at PB scale. It is sound only where data files are never rewritten in place at the
  same key and size; it cannot see such a rewrite. Metadata and non-Zarr files are always
  hashed in full (they are small), so a metadata edit is always caught.

Either way, file keys and metadata are canonicalised, so a ``dimension_separator`` or
``chunk_key_encoding`` change alone does not move the id, but a re-chunk, a re-shard, or a
re-compress does (the metadata and the file inventory change). The policy is named in the
id's format version, so a ``bytes`` id and an ``inventory`` id never look alike.

See ``data-versioning/design/zarr-content-hash.md`` and the 2026-10-16 / 2026-10-19
decisions.
"""
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor

from blake3 import blake3

META_NAMES = {".zarray", ".zgroup", ".zattrs", ".zmetadata", "zarr.json"}
ZARR_MARKERS = {".zgroup", ".zarray", "zarr.json"}
DROP_META_KEYS = {"dimension_separator", "chunk_key_encoding"}

READ_BLOCK = 4 << 20  # 4 MiB

# Id format version per leaf policy. It is part of the root hash, so the policy and any
# construction change are detectable. Bump when the construction changes.
FORMATS = {"bytes": "zarrsum-id/1", "inventory": "zarrsum-inv/1"}


# ---------- hashing primitives ----------
def _h(tag: str, items: list[bytes]) -> bytes:
    """Typed, length-prefixed BLAKE3 over a list of byte items."""
    hasher = blake3()
    t = tag.encode()
    hasher.update(len(t).to_bytes(8, "little"))
    hasher.update(t)
    for it in items:
        hasher.update(len(it).to_bytes(8, "little"))
        hasher.update(it)
    return hasher.digest()


def _hash_file_bytes(path: str) -> bytes:
    h = blake3(max_threads=blake3.AUTO)
    with open(path, "rb") as f:
        while True:
            b = f.read(READ_BLOCK)
            if not b:
                break
            h.update(b)
    return h.digest()


def _size_leaf(path: str) -> bytes:
    """An inventory leaf: hash the file size only (a stat, no byte read)."""
    return _h("size", [os.path.getsize(path).to_bytes(8, "little")])


def _hash_many(paths: list[str], max_workers, mode: str) -> dict[str, bytes]:
    """Map each path to a leaf digest. ``mode`` is 'bytes' (read and hash) or 'size' (stat).
    Parallel by default; file I/O, stat latency, and BLAKE3 all release the GIL, so threads
    overlap NFS latency. Order does not matter; the caller sorts before combining."""
    if not paths:
        return {}
    fn = _hash_file_bytes if mode == "bytes" else _size_leaf
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 4) * 4)
    if max_workers <= 1:
        return {p: fn(p) for p in paths}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        digests = list(ex.map(fn, paths))
    return dict(zip(paths, digests))


def _canon_json(path: str) -> bytes:
    try:
        with open(path, "rb") as f:
            obj = json.load(f)
    except (OSError, ValueError):
        return b""
    if isinstance(obj, dict):
        obj = {k: v for k, v in obj.items() if k not in DROP_META_KEYS}
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


# ---------- chunk-key classification ----------
def _array_encoding(array_dir: str, names: set[str]) -> dict:
    if "zarr.json" in names:  # v3
        try:
            with open(os.path.join(array_dir, "zarr.json")) as f:
                meta = json.load(f)
        except (OSError, ValueError):
            meta = {}
        cke = meta.get("chunk_key_encoding") or {}
        name = cke.get("name", "default")
        default_sep = "/" if name == "default" else "."
        sep = (cke.get("configuration") or {}).get("separator", default_sep)
        return {"sep": sep, "prefix": "c" if name == "default" else "", "kind": "v3"}
    try:
        with open(os.path.join(array_dir, ".zarray")) as f:
            meta = json.load(f)
    except (OSError, ValueError):
        meta = {}
    return {"sep": meta.get("dimension_separator", "."), "prefix": "", "kind": "v2"}


def _data_file_key(rel: str, enc: dict):
    """Canonical key ('0.0.0') of a data file (chunk, or shard for sharded v3), else None.
    The shard is a leaf; it is not opened. Coords are joined on disk by ``enc['sep']``."""
    if enc["sep"] == "/":
        parts = rel.split(os.sep)
    else:
        if os.sep in rel:
            return None
        parts = rel.split(enc["sep"])
    if enc["prefix"]:
        if not parts or parts[0] != enc["prefix"]:
            return None
        parts = parts[1:]
    if not all(p.isdigit() for p in parts):
        return None
    return ".".join(parts)


def _is_array(names: set[str], d: str) -> bool:
    if ".zarray" in names:
        return True
    if "zarr.json" in names:
        try:
            with open(os.path.join(d, "zarr.json")) as f:
                return json.load(f).get("node_type") == "array"
        except (OSError, ValueError):
            return False
    return False


def _array_meta_items(d: str, names: set[str]) -> list[bytes]:
    items = []
    for m in (".zarray", "zarr.json", ".zattrs"):
        if m in names:
            items.append(m.encode() + b"=" + _canon_json(os.path.join(d, m)))
    return items


# ---------- phase 1: discover the tree and the file list ----------
def _discover_array(d: str, enc: dict, nonzarr: list[str]) -> dict:
    names = set(os.listdir(d))
    leaves = []  # (canon_file_key, abspath)
    for dp, _dn, fn in os.walk(d):
        for f in fn:
            ap = os.path.join(dp, f)
            if f in META_NAMES and dp == d:
                continue
            key = _data_file_key(os.path.relpath(ap, d), enc)
            if key is not None:
                leaves.append((key, ap))
            else:
                nonzarr.append(ap)
    return {"kind": "array", "abs": d, "meta": _array_meta_items(d, names), "leaves": leaves}


def _discover_group(d: str, nonzarr: list[str]) -> dict:
    names = set(os.listdir(d))
    meta = []
    for m in (".zgroup", "zarr.json", ".zattrs"):
        if m in names:
            meta.append(m.encode() + b"=" + _canon_json(os.path.join(d, m)))
    children = []  # (name, node), in sorted-name order
    for name in sorted(names):
        p = os.path.join(d, name)
        if os.path.isdir(p):
            cnames = set(os.listdir(p))
            if _is_array(cnames, p):
                children.append((name, _discover_array(p, _array_encoding(p, cnames), nonzarr)))
            else:
                children.append((name, _discover_group(p, nonzarr)))
        elif name not in META_NAMES:
            nonzarr.append(p)
    return {"kind": "group", "abs": d, "meta": meta, "children": children}


# ---------- phase 3: combine using the digest map ----------
def _combine(node: dict, dig: dict, arrays: list, groups: list, root: str) -> bytes:
    if node["kind"] == "array":
        leaves = sorted((k, dig[ap]) for k, ap in node["leaves"])
        leaf_ser = _h("files", [k.encode() + b"\0" + hv for k, hv in leaves])
        aid = _h("array", node["meta"] + [leaf_ser])
        arrays.append((os.path.relpath(node["abs"], root), aid, len(leaves)))
        return aid
    children = [(name, _combine(c, dig, arrays, groups, root)) for name, c in node["children"]]
    child_ser = _h("children", [n.encode() + b"\0" + cid for n, cid in children])
    gid = _h("group", node["meta"] + [child_ser])
    groups.append((os.path.relpath(node["abs"], root), gid))
    return gid


def hash_dataset(root: str, max_workers=None, policy: str = "bytes") -> dict:
    """Hash a dataset tree. ``root`` may be one Zarr store or a directory of stores and
    other files. ``policy`` is 'bytes' (default, always sound) or 'inventory' (data-file
    leaf = size, no byte read; see the module docstring). ``max_workers`` sizes the file
    thread pool (None = auto, 1 = serial); the id does not depend on it. Returns the dataset
    id, per-store ids, per-group ids, and per-array ids; all values are JSON-safe."""
    if policy not in FORMATS:
        raise ValueError(f"policy must be one of {sorted(FORMATS)}")

    # Phase 1: one walk builds the tree and the file list, without reading bytes.
    nonzarr = []  # abspaths
    stores = []  # (relpath, node)
    for dp, dn, fn in os.walk(root):
        names = set(fn) | set(dn)
        if names & ZARR_MARKERS:
            cnames = set(os.listdir(dp))
            if _is_array(cnames, dp):
                node = _discover_array(dp, _array_encoding(dp, cnames), nonzarr)
            else:
                node = _discover_group(dp, nonzarr)
            stores.append((os.path.relpath(dp, root), node))
            dn[:] = []
        else:
            for f in fn:
                nonzarr.append(os.path.join(dp, f))

    # Phase 2: data-file leaves use the policy; metadata (in _combine) and non-Zarr files are
    # always hashed in full, because they are small and may be edited in place.
    leaf_abs = []

    def _collect(n):
        if n["kind"] == "array":
            leaf_abs.extend(ap for _k, ap in n["leaves"])
        else:
            for _name, c in n["children"]:
                _collect(c)

    for _rel, node in stores:
        _collect(node)
    mode = "size" if policy == "inventory" else "bytes"
    dig = _hash_many(leaf_abs, max_workers, mode)
    dig.update(_hash_many(nonzarr, max_workers, "bytes"))

    # Phase 3: combine.
    arrays, groups = [], []
    store_ids = []
    for rel, node in stores:
        store_ids.append((rel, _combine(node, dig, arrays, groups, root)))
    store_ids.sort()
    nz = sorted((os.path.relpath(p, root), dig[p]) for p in nonzarr)
    fmt = FORMATS[policy]
    store_ser = _h("stores", [r.encode() + b"\0" + i for r, i in store_ids])
    nz_ser = _h("nonzarr", [r.encode() + b"\0" + h for r, h in nz])
    dataset_id = _h("dataset", [fmt.encode(), store_ser, nz_ser])
    return {
        "policy": policy,
        "format": fmt,
        "dataset_id": dataset_id.hex(),
        "stores": [(r, i.hex()) for r, i in store_ids],
        "groups": [(r, i.hex()) for r, i in sorted(groups)],
        "arrays": [(r, i.hex(), n) for r, i, n in sorted(arrays)],
        "n_stores": len(store_ids),
        "n_groups": len(groups),
        "n_arrays": len(arrays),
        "n_files": sum(n for _, _, n in arrays),
        "n_nonzarr": len(nz),
    }
