"""Core of the Zarr content id.

The id is a BLAKE3 Merkle tree over the chunk/shard files on disk and the canonical
Zarr metadata: chunk -> array -> group -> store, then a dataset id that combines the
store ids with the non-Zarr files.

Chunk bytes are hashed exactly. Chunk keys and metadata are canonicalised, so a
``dimension_separator`` or ``chunk_key_encoding`` change alone does not move the id, but a
re-chunk or a re-compress does. The hasher never decodes a chunk.

See ``data-versioning/design/zarr-content-hash.md`` and the 2026-10-16 decisions.
"""
from __future__ import annotations

import json
import os

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
    """Return how chunk keys are encoded for this array: {sep, prefix, kind}."""
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
        if os.sep in rel:  # a '.'-encoded chunk is one file, never nested
            return None
        parts = rel.split(enc["sep"])
    if enc["prefix"]:
        if not parts or parts[0] != enc["prefix"]:
            return None
        parts = parts[1:]
    if not all(p.isdigit() for p in parts):  # all() of [] is True -> scalar chunk ""
        return None
    return ".".join(parts)


# ---------- node hashing ----------
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


def _hash_array(d: str, enc: dict, acc) -> bytes:
    """Hash one array directory. Each data file (a chunk, or a shard for sharded v3) is a
    leaf. ``acc`` collects arrays and non-Zarr sidecars."""
    names = set(os.listdir(d))
    leaves = []  # (canon_file_key, file_hash)
    for dp, _dn, fn in os.walk(d):
        for f in fn:
            ap = os.path.join(dp, f)
            if f in META_NAMES and dp == d:
                continue  # array metadata, hashed via _array_meta_items
            rel = os.path.relpath(ap, d)
            key = _data_file_key(rel, enc)
            if key is not None:
                leaves.append((key, _hash_file(ap)))
            else:
                acc["nonzarr"].append((os.path.relpath(ap, acc["root"]), _hash_file(ap)))
    leaves.sort()
    leaf_ser = _h("files", [k.encode() + b"\0" + hv for k, hv in leaves])
    aid = _h("array", _array_meta_items(d, names) + [leaf_ser])
    acc["arrays"].append((os.path.relpath(d, acc["root"]), aid, len(leaves)))
    return aid


def _hash_group(d: str, acc) -> bytes:
    names = set(os.listdir(d))
    meta = []
    for m in (".zgroup", "zarr.json", ".zattrs"):
        if m in names:
            meta.append(m.encode() + b"=" + _canon_json(os.path.join(d, m)))
    children = []  # (name, id)
    for name in sorted(names):
        p = os.path.join(d, name)
        if os.path.isdir(p):
            cnames = set(os.listdir(p))
            if _is_array(cnames, p):
                children.append((name, _hash_array(p, _array_encoding(p, cnames), acc)))
            else:
                children.append((name, _hash_group(p, acc)))
        elif name not in META_NAMES:
            acc["nonzarr"].append((os.path.relpath(p, acc["root"]), _hash_file(p)))
    child_ser = _h("children", [n.encode() + b"\0" + cid for n, cid in children])
    return _h("group", meta + [child_ser])


def hash_dataset(root: str) -> dict:
    """Hash a dataset tree. ``root`` may be one Zarr store or a directory that holds many
    stores and non-Zarr files. Returns the dataset id, per-store ids, and per-array ids.

    The result keys are all JSON-safe (hex strings and ints)."""
    acc = {"root": root, "arrays": [], "nonzarr": []}
    stores = []  # (relpath, store_id)
    for dp, dn, fn in os.walk(root):
        names = set(fn) | set(dn)
        # The shallowest dir on a path that holds a Zarr marker is a store root. os.walk
        # is top-down and we prune below, so nested group markers are not seen here. Do
        # not rely on a ".zarr" suffix: it is only a convention.
        if names & ZARR_MARKERS:
            cnames = set(os.listdir(dp))
            if _is_array(cnames, dp):
                sid = _hash_array(dp, _array_encoding(dp, cnames), acc)
            else:
                sid = _hash_group(dp, acc)
            stores.append((os.path.relpath(dp, root), sid))
            dn[:] = []  # hashed recursively; do not descend
        else:
            for f in fn:
                ap = os.path.join(dp, f)
                acc["nonzarr"].append((os.path.relpath(ap, root), _hash_file(ap)))
    stores.sort()
    acc["nonzarr"].sort()
    store_ser = _h("stores", [r.encode() + b"\0" + i for r, i in stores])
    nz_ser = _h("nonzarr", [r.encode() + b"\0" + h for r, h in acc["nonzarr"]])
    dataset_id = _h("dataset", [FORMAT.encode(), store_ser, nz_ser])
    return {
        "format": FORMAT,
        "dataset_id": dataset_id.hex(),
        "stores": [(r, i.hex()) for r, i in stores],
        "arrays": [(r, i.hex(), n) for r, i, n in sorted(acc["arrays"])],
        "n_stores": len(stores),
        "n_arrays": len(acc["arrays"]),
        "n_files": sum(n for _, _, n in acc["arrays"]),
        "n_nonzarr": len(acc["nonzarr"]),
    }
