"""Core of the Zarr content id.

The id is a BLAKE3 Merkle tree over the data files on disk and the canonical Zarr
metadata: file -> array -> group -> store, then a dataset id that combines the store ids
with the non-Zarr files. A leaf is one data file: one chunk for an unsharded array, one
shard for a sharded Zarr v3 array (the shard is not opened).

Two leaf policies:

- ``bytes`` (default): a data-file leaf is ``BLAKE3(file bytes)``. Always sound; reads
  every byte.
- ``inventory``: a data-file leaf is ``BLAKE3(file size)`` -- a stat, no byte read. Near
  free at PB scale. Sound where data files are never rewritten in place at the same key and
  size. Metadata and non-Zarr files are always hashed in full, so a metadata edit is caught.

Work runs in two phases. Discovery crawls the tree and lists files; it is parallel by
default (a concurrent ``scandir`` BFS plus a concurrent metadata prefetch), because on a
network filesystem the per-directory ``readdir`` latency, not the bytes, dominates. The
combine phase is pure in-memory hashing. The id does not depend on the worker count or the
walk order. A serial discovery path is kept for cross-checking.

See ``data-versioning/design/zarr-content-hash.md`` and the 2026-10-16 / 2026-10-19 /
2026-10-20 decisions.
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
FORMATS = {"bytes": "zarrsum-id/1", "inventory": "zarrsum-inv/1"}


def _default_workers() -> int:
    return min(32, (os.cpu_count() or 4) * 4)


# ---------- hashing primitives ----------
def _h(tag: str, items: list[bytes]) -> bytes:
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


def _run(fn, items, workers):
    if not items:
        return []
    if workers <= 1:
        return [fn(x) for x in items]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(fn, items))


def _hash_many(paths: list[str], workers: int, mode: str) -> dict[str, bytes]:
    fn = _hash_file_bytes if mode == "bytes" else _size_leaf
    return dict(zip(paths, _run(fn, paths, workers)))


def _canon_bytes(b: bytes) -> bytes:
    try:
        obj = json.loads(b)
    except (ValueError, TypeError):
        return b""
    if isinstance(obj, dict):
        obj = {k: v for k, v in obj.items() if k not in DROP_META_KEYS}
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


# ---------- chunk-key classification ----------
def _encoding_from_meta(meta: dict, is_v3: bool) -> dict:
    if is_v3:
        cke = meta.get("chunk_key_encoding") or {}
        name = cke.get("name", "default")
        default_sep = "/" if name == "default" else "."
        sep = (cke.get("configuration") or {}).get("separator", default_sep)
        return {"sep": sep, "prefix": "c" if name == "default" else "", "kind": "v3"}
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


# ---------- parallel discovery ----------
def _scan_dir(d: str):
    subdirs, files = [], []
    try:
        with os.scandir(d) as it:
            for e in it:
                try:
                    if e.is_dir(follow_symlinks=False):
                        subdirs.append(e.name)
                    else:
                        files.append(e.name)
                except OSError:
                    files.append(e.name)
    except OSError:
        pass
    return d, subdirs, files


def _crawl(root: str, workers: int) -> dict:
    """Concurrent level-synchronous BFS. Returns {dir: (subdirs, files)} for every directory
    reachable from root. All directories at one depth are listed at once."""
    entries = {}
    pending = [root]
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        while pending:
            results = ex.map(_scan_dir, pending) if workers > 1 else map(_scan_dir, pending)
            pending = []
            for d, subs, files in results:
                entries[d] = (subs, files)
                pending.extend(os.path.join(d, s) for s in subs)
    return entries


def _prefetch_meta(entries: dict, workers: int) -> dict:
    paths = [os.path.join(d, f) for d, (_s, files) in entries.items()
             for f in files if f in META_NAMES]

    def rd(p):
        try:
            with open(p, "rb") as fh:
                return p, fh.read()
        except OSError:
            return p, b""

    return dict(_run(rd, paths, workers))


def _is_array_cached(d: str, files: list, cache: dict) -> bool:
    if ".zarray" in files:
        return True
    if "zarr.json" in files:
        try:
            return json.loads(cache.get(os.path.join(d, "zarr.json"), b"") or b"{}").get("node_type") == "array"
        except ValueError:
            return False
    return False


def _enc_cached(d: str, files: list, cache: dict) -> dict:
    if "zarr.json" in files:
        try:
            meta = json.loads(cache.get(os.path.join(d, "zarr.json"), b"") or b"{}")
        except ValueError:
            meta = {}
        return _encoding_from_meta(meta, is_v3=True)
    try:
        meta = json.loads(cache.get(os.path.join(d, ".zarray"), b"") or b"{}")
    except ValueError:
        meta = {}
    return _encoding_from_meta(meta, is_v3=False)


def _meta_items(d: str, files: list, cache: dict, order) -> list[bytes]:
    items = []
    for m in order:
        if m in files:
            items.append(m.encode() + b"=" + _canon_bytes(cache.get(os.path.join(d, m), b"")))
    return items


def _build_array(d: str, enc: dict, entries: dict, cache: dict, nonzarr: list) -> dict:
    leaves = []
    stack = [d]
    while stack:
        cur = stack.pop()
        subs, files = entries[cur]
        for f in files:
            ap = os.path.join(cur, f)
            if f in META_NAMES and cur == d:
                continue
            key = _data_file_key(os.path.relpath(ap, d), enc)
            (leaves.append((key, ap)) if key is not None else nonzarr.append(ap))
        for s in subs:
            stack.append(os.path.join(cur, s))
    meta = _meta_items(d, entries[d][1], cache, (".zarray", "zarr.json", ".zattrs"))
    return {"kind": "array", "abs": d, "meta": meta, "leaves": leaves}


def _build_group(d: str, entries: dict, cache: dict, nonzarr: list) -> dict:
    subs, files = entries[d]
    meta = _meta_items(d, files, cache, (".zgroup", "zarr.json", ".zattrs"))
    children = []
    for name in sorted(subs):
        p = os.path.join(d, name)
        psubs, pfiles = entries[p]
        if _is_array_cached(p, pfiles, cache):
            children.append((name, _build_array(p, _enc_cached(p, pfiles, cache), entries, cache, nonzarr)))
        else:
            children.append((name, _build_group(p, entries, cache, nonzarr)))
    for f in sorted(files):
        if f not in META_NAMES:
            nonzarr.append(os.path.join(d, f))
    return {"kind": "group", "abs": d, "meta": meta, "children": children}


def _discover_parallel(root: str, workers: int):
    entries = _crawl(root, workers)
    cache = _prefetch_meta(entries, workers)
    stores, nonzarr = [], []
    stack = [root]
    while stack:
        d = stack.pop()
        subs, files = entries[d]
        if set(files) & ZARR_MARKERS:
            if _is_array_cached(d, files, cache):
                node = _build_array(d, _enc_cached(d, files, cache), entries, cache, nonzarr)
            else:
                node = _build_group(d, entries, cache, nonzarr)
            stores.append((os.path.relpath(d, root), node))
        else:
            for f in files:
                nonzarr.append(os.path.join(d, f))
            for s in subs:
                stack.append(os.path.join(d, s))
    return stores, nonzarr


# ---------- phase 3: combine ----------
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
    """Hash a dataset tree. ``policy`` is 'bytes' (default) or 'inventory'. ``max_workers``
    sizes the discovery and hashing thread pools (None = auto, 1 = serial); the id does not
    depend on it."""
    if policy not in FORMATS:
        raise ValueError(f"policy must be one of {sorted(FORMATS)}")
    w = _default_workers() if max_workers is None else max(1, max_workers)
    stores, nonzarr = _discover_parallel(root, w)

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
    dig = _hash_many(leaf_abs, w, mode)
    dig.update(_hash_many(nonzarr, w, "bytes"))

    arrays, groups, store_ids = [], [], []
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
