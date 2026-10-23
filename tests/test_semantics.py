"""Semantic guarantees of the id (design section 2).

Run with pytest, or directly: ``python tests/test_semantics.py``.
"""
import json
import os
import tempfile

import pytest

import zarrsum
import zarrsum.core as core


def _build(d, sep, chunks, chunkbytes):
    """Write a minimal Zarr v2 store with one array named 'a'."""
    os.makedirs(os.path.join(d, "a"))
    with open(os.path.join(d, ".zgroup"), "w") as f:
        json.dump({"zarr_format": 2}, f)
    with open(os.path.join(d, "a", ".zarray"), "w") as f:
        json.dump(
            {
                "zarr_format": 2, "shape": [4, 4], "chunks": chunks, "dtype": "<i4",
                "fill_value": 0, "order": "C", "filters": None, "compressor": None,
                "dimension_separator": sep,
            },
            f,
        )
    for key, b in chunkbytes.items():
        parts = key.split(".")
        if sep == "/":
            p = os.path.join(d, "a", *parts)
            os.makedirs(os.path.dirname(p), exist_ok=True)
        else:
            p = os.path.join(d, "a", sep.join(parts))
        with open(p, "wb") as f:
            f.write(b)
    return d


def test_separator_independent_and_rechunk_sensitive():
    cb = {f"{i}.{j}": bytes([i * 2 + j]) * 16 for i in range(2) for j in range(2)}
    with tempfile.TemporaryDirectory() as base:
        s_dot = _build(os.path.join(base, "dot"), ".", [2, 2], cb)
        s_slash = _build(os.path.join(base, "slash"), "/", [2, 2], cb)
        s_rechunk = _build(
            os.path.join(base, "rechunk"), ".", [4, 4],
            {"0.0": b"".join(cb[k] for k in sorted(cb))},
        )
        id_dot = zarrsum.hash_dataset(s_dot)["dataset_id"]
        id_slash = zarrsum.hash_dataset(s_slash)["dataset_id"]
        id_rechunk = zarrsum.hash_dataset(s_rechunk)["dataset_id"]

    assert id_dot == id_slash, "separator alone must not change the id"
    assert id_dot != id_rechunk, "a re-chunk must change the id"


def test_deterministic():
    cb = {"0.0": b"\x01" * 16}
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], cb)
        assert zarrsum.hash_dataset(s)["dataset_id"] == zarrsum.hash_dataset(s)["dataset_id"]


def _build_v3_sharded(d, encoding, shard_bytes):
    """Write a minimal Zarr v3 array that uses the sharding_indexed codec, with one array
    'a'. Shard shape [2,4] over shape [4,4] gives shard keys '0.0' and '1.0'. The shard
    bytes are arbitrary: zarrsum never decodes a shard. `encoding` is 'default' (c/ prefix,
    '/' separator) or 'v2' (no prefix, '.' separator)."""
    os.makedirs(os.path.join(d, "a"))
    with open(os.path.join(d, "zarr.json"), "w") as f:
        json.dump({"zarr_format": 3, "node_type": "group", "attributes": {}}, f)
    cke = ({"name": "default", "configuration": {"separator": "/"}} if encoding == "default"
           else {"name": "v2", "configuration": {"separator": "."}})
    with open(os.path.join(d, "a", "zarr.json"), "w") as f:
        json.dump(
            {
                "zarr_format": 3, "node_type": "array", "shape": [4, 4], "data_type": "int32",
                "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [2, 4]}},
                "chunk_key_encoding": cke, "fill_value": 0,
                "codecs": [{"name": "sharding_indexed", "configuration": {
                    "chunk_shape": [2, 2],
                    "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
                    "index_codecs": [{"name": "bytes", "configuration": {"endian": "little"}},
                                     {"name": "crc32c"}],
                    "index_location": "end"}}],
                "attributes": {},
            },
            f,
        )
    for key, b in shard_bytes.items():
        coords = key.split(".")
        if encoding == "default":
            p = os.path.join(d, "a", "c", *coords)
            os.makedirs(os.path.dirname(p), exist_ok=True)
        else:
            p = os.path.join(d, "a", ".".join(coords))
        with open(p, "wb") as f:
            f.write(b)
    return d


def test_v3_sharded_hashes_shard_files():
    sb = {"0.0": b"\xaa" * 64, "1.0": b"\xbb" * 64}
    with tempfile.TemporaryDirectory() as base:
        s = _build_v3_sharded(os.path.join(base, "s"), "default", sb)
        r = zarrsum.hash_dataset(s)
    # two shard files are two leaves (not the four inner chunks)
    assert r["n_stores"] == 1 and r["n_arrays"] == 1
    assert r["n_files"] == 2, r
    assert r["n_nonzarr"] == 0, r


def test_v3_key_encoding_independent():
    sb = {"0.0": b"\xaa" * 64, "1.0": b"\xbb" * 64}
    with tempfile.TemporaryDirectory() as base:
        s_default = _build_v3_sharded(os.path.join(base, "d"), "default", sb)
        s_v2enc = _build_v3_sharded(os.path.join(base, "v"), "v2", sb)
        assert (zarrsum.hash_dataset(s_default)["dataset_id"]
                == zarrsum.hash_dataset(s_v2enc)["dataset_id"])


def test_worker_count_invariant():
    cb = {f"{i}.{j}": bytes([i * 2 + j]) * 16 for i in range(2) for j in range(2)}
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), "/", [2, 2], cb)
        serial = zarrsum.hash_dataset(s, max_workers=1)["dataset_id"]
        parallel = zarrsum.hash_dataset(s, max_workers=8)["dataset_id"]
    assert serial == parallel, "the id must not depend on the worker count"


def test_group_ids_present():
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), "/", [2, 2], {"0.0": b"\x01" * 16})
        r = zarrsum.hash_dataset(s)
    assert r["n_groups"] >= 1 and r["groups"], r


def test_inventory_differs_from_bytes():
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], {"0.0": b"\x01" * 16})
        assert (zarrsum.hash_dataset(s, policy="bytes")["dataset_id"]
                != zarrsum.hash_dataset(s, policy="inventory")["dataset_id"])


def test_inventory_sees_same_size_rewrite_via_mtime():
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], {"0.0": b"\x01" * 16})
        chunk = os.path.join(s, "a", "0.0")
        inv1 = zarrsum.hash_dataset(s, policy="inventory")["dataset_id"]
        m1 = os.stat(chunk).st_mtime_ns
        with open(chunk, "wb") as f:  # same size (16), new bytes -> mtime bumps
            f.write(b"\x02" * 16)
        inv2 = zarrsum.hash_dataset(s, policy="inventory")["dataset_id"]
        assert inv2 != inv1, "inventory must see a same-size rewrite (mtime bumped)"
        # the remaining blind spot: new bytes, same size, mtime restored to inv1's value
        with open(chunk, "wb") as f:
            f.write(b"\x03" * 16)
        os.utime(chunk, ns=(m1, m1))
        inv_blind = zarrsum.hash_dataset(s, policy="inventory")["dataset_id"]
        assert inv_blind == inv1, "same size + restored mtime -> inventory id back to inv1 (blind)"


def test_inventory_sees_size_change_and_metadata_edit():
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], {"0.0": b"\x01" * 16})
        inv0 = zarrsum.hash_dataset(s, policy="inventory")["dataset_id"]
        with open(os.path.join(s, "a", "0.0"), "wb") as f:  # different size
            f.write(b"\x01" * 32)
        inv_sz = zarrsum.hash_dataset(s, policy="inventory")["dataset_id"]
        za = os.path.join(s, "a", ".zarray")
        meta = json.load(open(za)); meta["fill_value"] = 7; json.dump(meta, open(za, "w"))
        inv_meta = zarrsum.hash_dataset(s, policy="inventory")["dataset_id"]
    assert inv_sz != inv0, "inventory must see a data-file size change"
    assert inv_meta != inv_sz, "inventory must see a metadata edit (metadata is hashed in full)"


def _build_nested(base):
    """A store with a subgroup, two arrays, and a non-Zarr sidecar."""
    d = os.path.join(base, "nested")
    for sub in ("g/a1", "g/a2"):
        os.makedirs(os.path.join(d, sub))
    json.dump({"zarr_format": 2}, open(os.path.join(d, ".zgroup"), "w"))
    json.dump({"zarr_format": 2}, open(os.path.join(d, "g", ".zgroup"), "w"))
    for a, val in (("a1", 1), ("a2", 2)):
        ad = os.path.join(d, "g", a)
        json.dump({"zarr_format": 2, "shape": [4, 4], "chunks": [2, 2], "dtype": "<i4",
                   "fill_value": 0, "order": "C", "filters": None, "compressor": None,
                   "dimension_separator": "/"}, open(os.path.join(ad, ".zarray"), "w"))
        for i in range(2):
            os.makedirs(os.path.join(ad, str(i)))
            with open(os.path.join(ad, str(i), "0"), "wb") as f:
                f.write(bytes([val]) * 32)
    with open(os.path.join(d, "g", "a1", "notes.json"), "w") as f:
        f.write('{"sidecar": true}')  # non-Zarr sidecar inside an array dir
    return d


def test_nested_store_worker_invariant():
    """A nested store (subgroup, two arrays, a sidecar): the id must not depend on the
    worker count (so the parallel crawl at 1 vs many workers agrees), for both policies."""
    with tempfile.TemporaryDirectory() as base:
        d = _build_nested(base)
        for pol in ("bytes", "inventory"):
            one = zarrsum.hash_dataset(d, policy=pol, max_workers=1)
            many = zarrsum.hash_dataset(d, policy=pol, max_workers=8)
            assert one["dataset_id"] == many["dataset_id"], (pol, one["dataset_id"], many["dataset_id"])
        r = zarrsum.hash_dataset(d)
        assert r["n_arrays"] == 2 and r["n_groups"] >= 2 and r["n_nonzarr"] == 1, r  # sidecar counted


def test_symlinks_are_skipped():
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), "/", [2, 2], {"0.0": b"\x01" * 16})
        base_id = zarrsum.hash_dataset(s)["dataset_id"]
        # a symlink to an external dir and a symlink to an external file, inside the store
        ext_dir = os.path.join(base, "extdir"); os.makedirs(ext_dir)
        with open(os.path.join(base, "extfile"), "wb") as f:
            f.write(b"x" * 10)
        os.symlink(ext_dir, os.path.join(s, "link_to_dir"))     # would crash if opened as a file
        os.symlink(os.path.join(base, "extfile"), os.path.join(s, "a", "link_to_file"))
        r = zarrsum.hash_dataset(s)  # must not raise
        assert r["dataset_id"] == base_id, "symlinks must not change the id (they are skipped)"


def test_ignore_transient_stores():
    with tempfile.TemporaryDirectory() as base:
        ds = os.path.join(base, "ds")
        _build(os.path.join(ds, "real.zarr"), "/", [2, 2], {"0.0": b"\x01" * 16})
        only_real = zarrsum.hash_dataset(ds)["dataset_id"]
        # a transient store that must be ignored by default
        _build(os.path.join(ds, ".trash_seg.zarr_temp_seg_1"), "/", [2, 2], {"0.0": b"\x09" * 16})
        assert zarrsum.hash_dataset(ds)["dataset_id"] == only_real, "default ignore must skip it"
        assert zarrsum.hash_dataset(ds, ignore=())["n_stores"] == 2, "no-ignore sees both"


def test_incremental_cache():
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], {"0.0": b"\x01" * 16})
        cache = os.path.join(base, "cache.json")
        id1 = zarrsum.hash_dataset(s, policy="bytes", cache=cache)["dataset_id"]
        assert os.path.exists(cache)
        assert zarrsum.hash_dataset(s, policy="bytes", cache=cache)["dataset_id"] == id1  # reuse
        chunk = os.path.join(s, "a", "0.0")
        with open(chunk, "wb") as f:
            f.write(b"\x02" * 16)  # same size, new bytes, fresh mtime
        id2 = zarrsum.hash_dataset(s, policy="bytes", cache=cache)["dataset_id"]
        assert id2 != id1, "a changed file (new mtime) must change the id"
        # stale-cache case: change bytes but restore the cached mtime, so (size, mtime) match
        m = os.stat(chunk).st_mtime_ns  # this mtime is now in the cache (for the \x02 bytes)
        with open(chunk, "wb") as f:
            f.write(b"\x03" * 16)  # new bytes, same size
        os.utime(chunk, ns=(m, m))  # restore mtime -> cache will trust and reuse
        stale = zarrsum.hash_dataset(s, policy="bytes", cache=cache)["dataset_id"]
        fresh = zarrsum.hash_dataset(s, policy="bytes", cache=cache, verify=True)["dataset_id"]
        assert stale == id2, "cache trusts (size, mtime): same mtime -> reuses the old hash"
        assert fresh != id2, "verify re-reads the bytes and sees the change"


def test_fence_passes_when_quiescent():
    """The stat fence must not change the id and must not trip on a store nobody is writing."""
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], {"0.0": b"\x01" * 16})
        plain = zarrsum.hash_dataset(s)["dataset_id"]
        fenced = zarrsum.hash_dataset(s, fence=True)
        assert fenced["dataset_id"] == plain, "the fence must not change the id"


def test_fence_detects_change_during_hashing(monkeypatch):
    """A file that changes between discovery and the post-hash re-stat must abort the run."""
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], {"0.0": b"\x01" * 16})
        chunk = os.path.join(s, "a", "0.0")
        orig = core._hash_file_bytes

        def mutating(path):
            h = orig(path)
            if os.path.abspath(path) == os.path.abspath(chunk):
                with open(chunk, "ab") as f:  # append mid-hash -> size and mtime change
                    f.write(b"\x02")
            return h

        monkeypatch.setattr(core, "_hash_file_bytes", mutating)
        with pytest.raises(core.DatasetChangedError):
            zarrsum.hash_dataset(s, policy="bytes", fence=True, max_workers=1)


def test_min_quiet_refuses_recent_write():
    """The quiescence gate refuses a just-written store and allows one past the window."""
    with tempfile.TemporaryDirectory() as base:
        s = _build(os.path.join(base, "s"), ".", [2, 2], {"0.0": b"\x01" * 16})
        with pytest.raises(zarrsum.DatasetBusyError):
            zarrsum.hash_dataset(s, min_quiet_s=1000)  # written just now -> inside the window
        assert zarrsum.hash_dataset(s, min_quiet_s=0)["dataset_id"], "a zero window must pass"


if __name__ == "__main__":
    test_separator_independent_and_rechunk_sensitive()
    test_deterministic()
    test_v3_sharded_hashes_shard_files()
    test_v3_key_encoding_independent()
    test_worker_count_invariant()
    test_group_ids_present()
    test_inventory_differs_from_bytes()
    test_inventory_sees_same_size_rewrite_via_mtime()
    test_inventory_sees_size_change_and_metadata_edit()
    test_nested_store_worker_invariant()
    test_symlinks_are_skipped()
    test_ignore_transient_stores()
    test_incremental_cache()
    test_fence_passes_when_quiescent()
    test_min_quiet_refuses_recent_write()
    print("PASS")
