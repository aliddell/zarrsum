"""Core of the Zarr content id.

The id is a BLAKE3 Merkle tree over the data files on disk and the canonical Zarr
metadata: file -> array -> group -> store, then a dataset id that combines the store ids
with the non-Zarr files. A leaf is one data file: one chunk for an unsharded array, one
shard for a sharded Zarr v3 array (the shard is not opened).

Data bytes are hashed exactly. File keys and metadata are canonicalised, so a
``dimension_separator`` or ``chunk_key_encoding`` change alone does not move the id, but a
re-chunk, a re-shard, or a re-compress does. The hasher never decodes a chunk.

The work runs in two phases: a single walk discovers the tree and the file list, then the
files are hashed (in parallel by default, because file I/O and BLAKE3 both release the GIL),
then the tree is combined. The id does not depend on the worker count or the walk order.

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
# Metadata keys that only name files, not data or layout. Dropped so the id does not
# depend on the separator or the key encoding (design section 2, guarantee 1).
DROP_META_KEYS = {"dimension_separator", "chunk_key_encoding"}

READ_BLOCK = 4 << 20  # 4 MiB

# Id format version. It is part of the root hash, so a format change gives new ids and
# is detectable. Bump it whenever the construction below changes.
FORMAT = "zarrsum-id/1"


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


def _hash_file(path: str) -> bytes:
    h = blake3(max_threads=blake3.AUTO)
    with open(path, "rb") as f:
        while True:
            b = f.read(READ_BLOCK)
            if not b:
                break
            h.update(b)
    return h.digest()


def _hash_many(paths: list[str], max_workers) -> dict[str, bytes]:
    """Hash every path. Parallel by default; file I/O and BLAKE3 release the GIL, so
    threads overlap NFS latency. The result is a path -> digest map; order does not matter
    because the caller sorts before combining."""
    if not paths:
        return {}
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 4) * 4)
    if max_workers <= 1:
        return {p: _hash_file(p) for p in paths}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        digests = list(ex.map(_hash_file, paths))
    return dict(zip(paths, digests))


def _canon_json(path: str) -> bytes:
    """Canonical JSON bytes of a metadata file, with layout-naming keys dropped."""
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
    """Return how data-file keys are encoded for this array: {sep, prefix, kind}."""
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
    # v2
    try:
        with open(os.path.join(array_dir, ".zarray")) as f:
            meta = json.load(f)
    except (OSError, ValueError):
        meta = {}
    return {"sep": meta.get("dimension_separator", "."), "prefix": "", "kind": "v2"}


def _data_file_key(rel: str, enc: dict):
    """Return the canonical key ('0.0.0') of a data file if ``rel`` (a path relative to
    the array dir) is a chunk or shard file, else None. For an unsharded array one data
    file is one chunk; for a sharded Zarr v3 array one data file is one shard (which holds
    many inner chunks). Either way the file is a leaf: zarrsum hashes the file, it does not
    look inside a shard. The on-disk coords are joined by ``enc['sep']``: '/' means nested
    directories; any other separator means one file name."""
    if enc["sep"] == "/":
        parts = rel.split(os.sep)
    else:
        if os.sep in rel:  # a '.'-encoded file is one file, never nested
            return None
        parts = rel.split(enc["sep"])
    if enc["prefix"]:
        if not parts or parts[0] != enc["prefix"]:
            return None
        parts = parts[1:]
    if not all(p.isdigit() for p in parts):  # all() of [] is True -> scalar file ""
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
                continue  # array metadata, hashed via _array_meta_items
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
    return {"kind": "group", "meta": meta, "children": children}


# ---------- phase 3: combine using the digest map ----------
def _combine(node: dict, dig: dict, arrays: list, root: str) -> bytes:
    if node["kind"] == "array":
        leaves = sorted((k, dig[ap]) for k, ap in node["leaves"])
        leaf_ser = _h("files", [k.encode() + b"\0" + hv for k, hv in leaves])
        aid = _h("array", node["meta"] + [leaf_ser])
        arrays.append((os.path.relpath(node["abs"], root), aid, len(leaves)))
        return aid
    children = [(name, _combine(child, dig, arrays, root)) for name, child in node["children"]]
    child_ser = _h("children", [n.encode() + b"\0" + cid for n, cid in children])
    return _h("group", node["meta"] + [child_ser])


def hash_dataset(root: str, max_workers=None) -> dict:
    """Hash a dataset tree. ``root`` may be one Zarr store or a directory that holds many
    stores and non-Zarr files. ``max_workers`` controls the file-hashing thread pool
    (None = auto, 1 = serial). The id is independent of the worker count. Returns the
    dataset id, per-store ids, and per-array ids; all values are JSON-safe."""
    # Phase 1: one walk builds the tree and the file list, without reading bytes.
    nonzarr = []  # abspaths
    stores = []  # (relpath, node)
    for dp, dn, fn in os.walk(root):
        names = set(fn) | set(dn)
        # The shallowest dir on a path that holds a Zarr marker is a store root. os.walk is
        # top-down and we prune below, so nested group markers are not seen here. Do not
        # rely on a ".zarr" suffix: it is only a convention.
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

    # Phase 2: hash every data file and non-Zarr file (parallel by default).
    leaf_abs = []

    def _collect(n):
        if n["kind"] == "array":
            leaf_abs.extend(ap for _k, ap in n["leaves"])
        else:
            for _name, c in n["children"]:
                _collect(c)

    for _rel, node in stores:
        _collect(node)
    dig = _hash_many(leaf_abs + nonzarr, max_workers)

    # Phase 3: combine.
    arrays = []
    store_ids = []
    for rel, node in stores:
        store_ids.append((rel, _combine(node, dig, arrays, root)))
    store_ids.sort()
    nz = sorted((os.path.relpath(p, root), dig[p]) for p in nonzarr)
    store_ser = _h("stores", [r.encode() + b"\0" + i for r, i in store_ids])
    nz_ser = _h("nonzarr", [r.encode() + b"\0" + h for r, h in nz])
    dataset_id = _h("dataset", [FORMAT.encode(), store_ser, nz_ser])
    return {
        "format": FORMAT,
        "dataset_id": dataset_id.hex(),
        "stores": [(r, i.hex()) for r, i in store_ids],
        "arrays": [(r, i.hex(), n) for r, i, n in sorted(arrays)],
        "n_stores": len(store_ids),
        "n_arrays": len(arrays),
        "n_files": sum(n for _, _, n in arrays),
        "n_nonzarr": len(nz),
    }
