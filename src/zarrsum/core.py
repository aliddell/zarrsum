"""Core of the Zarr content id.

The id is a BLAKE3 Merkle tree over the data files on disk and the canonical Zarr
metadata: file -> array -> group -> store, then a dataset id that combines the store ids
with the non-Zarr files. A leaf is one data file: one chunk for an unsharded array, one
shard for a sharded Zarr v3 array (the shard is not opened).

Two leaf policies:

- ``bytes`` (default): a data-file leaf is ``BLAKE3(file bytes)``. Always sound; reads
  every byte.
- ``inventory``: a data-file leaf is ``BLAKE3(size, mtime_ns)`` -- a stat, no byte read. Near
  free at PB scale. An in-place rewrite bumps the mtime, so it is seen; the id is blind only
  to a rewrite that keeps both the size and the mtime (which needs the mtime deliberately
  preserved). Metadata and non-Zarr files are always hashed in full, so a metadata edit is
  caught.

Work runs in two phases. Discovery crawls the tree and lists files; it is parallel by
default (a concurrent ``scandir`` BFS plus a concurrent metadata prefetch), because on a
network filesystem the per-directory ``readdir`` latency, not the bytes, dominates. The
combine phase is pure in-memory hashing. The id does not depend on the worker count or the
walk order. A serial discovery path is kept for cross-checking.

See ``data-versioning/design/zarr-content-hash.md`` and the 2026-10-16 / 2026-10-19 /
2026-10-20 decisions.
"""
from __future__ import annotations

import fnmatch
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

from blake3 import blake3

META_NAMES = {".zarray", ".zgroup", ".zattrs", ".zmetadata", "zarr.json"}
ZARR_MARKERS = {".zgroup", ".zarray", "zarr.json"}
DROP_META_KEYS = {"dimension_separator", "chunk_key_encoding"}

READ_BLOCK = 4 << 20  # 4 MiB
FORMATS = {"bytes": "zarrsum-id/1", "inventory": "zarrsum-inv/2"}

# Directory names skipped during the crawl: transient pipeline stores that are not part of
# the dataset (e.g. the OPS `.trash_..._temp_seg_...` stores). Matched with fnmatch on the
# directory name. A skipped dir is excluded from the id entirely.
DEFAULT_IGNORE = (".trash*", "*_temp", "*_temp_*", "*.tmp")


def _ignored(name: str, patterns) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in patterns)


class DatasetChangedError(RuntimeError):
    """A file seen during discovery was gone, or changed, while we hashed it. The dataset is
    being written. A content id of a moving target is not well defined: quiesce writes and
    retry. The default hash path raises this only when a listed file disappears; the optional
    stat fence (``fence=True``) also raises it when a file's size or mtime changed during the
    discovery-to-hash window, so it catches an in-place edit, an append, or a metadata rewrite."""


class DatasetBusyError(RuntimeError):
    """The dataset was modified too recently to hash safely. With ``min_quiet_s`` set, the id
    is refused when the newest file mtime is inside the quiet window, so a write may still be
    in flight. This is a coarse quiescence gate, not a lock: quiesce writes and retry."""


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
    try:
        h = blake3(max_threads=blake3.AUTO)
        with open(path, "rb") as f:
            while True:
                b = f.read(READ_BLOCK)
                if not b:
                    break
                h.update(b)
        return h.digest()
    except FileNotFoundError:
        raise DatasetChangedError(f"{path} disappeared during hashing; quiesce writes and retry")


def _inventory_leaf(path: str) -> bytes:
    """An inventory leaf: hash (size, mtime_ns) -- a stat, no byte read. mtime is included so
    an in-place rewrite that keeps the same size is still seen (its mtime bumps). The id is
    then blind only to a rewrite that keeps both the size and the mtime, which needs the mtime
    deliberately preserved (decision 2026-10-20)."""
    try:
        st = os.stat(path)
    except FileNotFoundError:
        raise DatasetChangedError(f"{path} disappeared during hashing; quiesce writes and retry")
    return _h("size+mtime", [st.st_size.to_bytes(8, "little"),
                             st.st_mtime_ns.to_bytes(8, "little")])


def _stat_sig(path: str):
    """A file's ``(size, mtime_ns)``, or None if it is gone. Used by the stat fence and the
    quiescence gate; neither reads bytes."""
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    return (st.st_size, st.st_mtime_ns)


def _run(fn, items, workers):
    if not items:
        return []
    if workers <= 1:
        return [fn(x) for x in items]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(fn, items))


def _hash_many(paths: list[str], workers: int, mode: str) -> dict[str, bytes]:
    fn = _hash_file_bytes if mode == "bytes" else _inventory_leaf
    return dict(zip(paths, _run(fn, paths, workers)))


def _hash_bytes_cached(paths: list[str], root: str, workers: int, cache_path: str,
                       verify: bool) -> dict[str, bytes]:
    """Byte-policy hashing with a reuse cache keyed by (size, mtime_ns). A file whose size
    and mtime match the cache reuses its stored hash instead of being re-read, so a new
    version re-reads only the files that changed. ``verify`` ignores the old cache (re-reads
    everything) but still refreshes it. mtime_ns is exact, so float rounding is not an
    issue. The cache trusts mtime: a rewrite that keeps the same size and mtime is not seen
    (use ``verify``)."""
    old = {}
    if not verify and os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                old = json.load(f).get("files", {})
        except (OSError, ValueError):
            old = {}

    def leaf(ap):
        rel = os.path.relpath(ap, root)
        try:
            st = os.stat(ap)
        except FileNotFoundError:
            raise DatasetChangedError(f"{ap} disappeared during hashing; quiesce writes and retry")
        e = old.get(rel)
        if e and e[0] == st.st_size and e[1] == st.st_mtime_ns:
            h = bytes.fromhex(e[2])
        else:
            h = _hash_file_bytes(ap)
        return rel, st.st_size, st.st_mtime_ns, h

    rows = _run(leaf, paths, workers)
    dig = {}
    new = {}
    for ap, (rel, size, mtime_ns, h) in zip(paths, rows):
        dig[ap] = h
        new[rel] = [size, mtime_ns, h.hex()]
    tmp = cache_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"cache_version": 1, "files": new}, f)
    os.replace(tmp, cache_path)
    return dig


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
    """List one directory. Symlinks are not followed: a symlink (to a file or a dir) is
    skipped entirely, which avoids cycles, aliasing, and opening a symlinked dir as a file.
    So the id covers the real files in the tree, not symlink targets."""
    subdirs, files = [], []
    try:
        with os.scandir(d) as it:
            for e in it:
                try:
                    if e.is_symlink():
                        continue
                    if e.is_dir(follow_symlinks=False):
                        subdirs.append(e.name)
                    else:
                        files.append(e.name)
                except OSError:
                    continue
    except OSError:
        pass
    return d, subdirs, files


def _crawl(root: str, workers: int, ignore) -> dict:
    """Concurrent level-synchronous BFS. Returns {dir: (subdirs, files)} for every directory
    reachable from root, minus ignored directory names. All dirs at one depth are listed at
    once."""
    entries = {}
    pending = [root]
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        while pending:
            results = ex.map(_scan_dir, pending) if workers > 1 else map(_scan_dir, pending)
            pending = []
            for d, subs, files in results:
                subs = [s for s in subs if not _ignored(s, ignore)]
                entries[d] = (subs, files)
                pending.extend(os.path.join(d, s) for s in subs)
    return entries


def _prefetch_meta(entries: dict, workers: int):
    """Read every metadata file, and capture each file's ``(size, mtime_ns)`` at the moment it
    is read. The stat is taken before the read, so a change during the read is caught by the
    fence. Returns ``(cache, meta_sig)``: the bytes by path, and the discovery-time signatures
    by path. The fence and the quiescence gate need ``meta_sig``, because the metadata is read
    here, at discovery, not during the later hashing phase, so its signature cannot be collected
    with the data files just before hashing."""
    paths = [os.path.join(d, f) for d, (_s, files) in entries.items()
             for f in files if f in META_NAMES]

    def rd(p):
        try:
            sig = _stat_sig(p)
            with open(p, "rb") as fh:
                return p, fh.read(), sig
        except OSError:
            return p, b"", None

    rows = _run(rd, paths, workers)
    cache = {p: data for p, data, _sig in rows}
    meta_sig = {p: sig for p, _data, sig in rows if sig is not None}
    return cache, meta_sig


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


def _discover_parallel(root: str, workers: int, ignore):
    entries = _crawl(root, workers, ignore)
    cache, meta_sig = _prefetch_meta(entries, workers)
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
    return stores, nonzarr, meta_sig


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


def hash_dataset(root: str, max_workers=None, policy: str = "bytes", ignore=None,
                 cache=None, verify=False, fence=False, min_quiet_s=None) -> dict:
    """Hash a dataset tree. ``policy`` is 'bytes' (default) or 'inventory'. ``max_workers``
    sizes the thread pools (None = auto, 1 = serial); the id does not depend on it.
    ``ignore`` is a list of fnmatch patterns for directory names to skip (default:
    DEFAULT_IGNORE). ``cache`` is a path to a reuse cache (bytes policy only) so a new version
    re-reads only changed files; ``verify`` ignores the old cache but refreshes it.

    Two optional mid-write guards; neither changes the id:

    - ``min_quiet_s``: a quiescence gate. Raise ``DatasetBusyError`` if the newest mtime over
      the data, non-Zarr, and metadata files is within ``min_quiet_s`` seconds of now, i.e. a
      write may still be in flight. Coarse, not a lock.
    - ``fence``: a stat fence. Snapshot ``(size, mtime)`` of every file and re-stat after
      hashing; raise ``DatasetChangedError`` if any file changed or vanished during the window.
      The data and non-Zarr files are snapshotted before hashing; the per-node metadata is
      snapshotted at discovery, when its bytes are read (the stat is taken before the read), so
      a metadata rewrite during the run is caught too. It cannot see a file created after
      discovery."""
    if policy not in FORMATS:
        raise ValueError(f"policy must be one of {sorted(FORMATS)}")
    w = _default_workers() if max_workers is None else max(1, max_workers)
    ign = DEFAULT_IGNORE if ignore is None else tuple(ignore)
    stores, nonzarr, meta_sig = _discover_parallel(root, w, ign)

    leaf_abs = []

    def _collect(n):
        if n["kind"] == "array":
            leaf_abs.extend(ap for _k, ap in n["leaves"])
        else:
            for _name, c in n["children"]:
                _collect(c)

    for _rel, node in stores:
        _collect(node)

    data_paths = leaf_abs + nonzarr
    base_sig = None
    if fence or min_quiet_s is not None:
        base_sig = dict(zip(data_paths, _run(_stat_sig, data_paths, w)))
        base_sig.update(meta_sig)  # metadata signatures from discovery (stat taken before the read)
        if min_quiet_s is not None:
            newest = max((s[1] for s in base_sig.values() if s), default=0)
            if newest and time.time_ns() - newest < int(min_quiet_s * 1e9):
                age_s = (time.time_ns() - newest) / 1e9
                raise DatasetBusyError(
                    f"newest file changed {age_s:.1f}s ago, inside the {min_quiet_s}s quiet "
                    f"window; a write may be in flight. Quiesce writes and retry.")

    if policy == "bytes" and cache:
        dig = _hash_bytes_cached(leaf_abs, root, w, cache, verify)
    else:
        mode = "size" if policy == "inventory" else "bytes"
        dig = _hash_many(leaf_abs, w, mode)
    dig.update(_hash_many(nonzarr, w, "bytes"))

    if fence:
        fence_paths = data_paths + list(meta_sig)
        after = dict(zip(fence_paths, _run(_stat_sig, fence_paths, w)))
        changed = [p for p in fence_paths if after[p] != base_sig.get(p)]
        if changed:
            raise DatasetChangedError(
                f"{len(changed)} file(s) changed during hashing (e.g. {changed[0]}); the "
                f"dataset is being written. Quiesce writes and retry.")

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
