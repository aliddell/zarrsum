"""Frozen identity vectors: the executable form of SPEC.md.

Every other test asserts a *property* of the id (two inputs are equal or not). These tests pin
the actual id bytes to a constant, so a change to the wire format is caught even when the
properties still hold. If one of these fails, the on-disk id format changed: update SPEC.md and
bump the format tag deliberately, do not just paste the new hash.

Run with pytest, or directly: ``python tests/test_identity.py``.
"""
import json
import os
import tempfile

import pytest

import zarrsum


def _w(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "w" if isinstance(data, str) else "wb"
    with open(path, mode) as f:
        f.write(data)


# ---- the three canonical fixtures (byte-for-byte as SPEC.md section 4) ----
def build_v2_array(root):
    _w(os.path.join(root, ".zarray"), json.dumps({
        "zarr_format": 2, "shape": [4], "chunks": [2], "dtype": "<i4",
        "fill_value": 0, "order": "C", "filters": None, "compressor": None,
        "dimension_separator": ".",
    }))
    _w(os.path.join(root, ".zattrs"), json.dumps({"scale": [1, 2]}))
    _w(os.path.join(root, "0"), bytes(range(0, 8)))
    _w(os.path.join(root, "1"), bytes(range(8, 16)))
    return root


def build_v2_group(root):
    _w(os.path.join(root, ".zgroup"), json.dumps({"zarr_format": 2}))
    _w(os.path.join(root, ".zattrs"), json.dumps({"name": "root"}))
    _w(os.path.join(root, "g", ".zgroup"), json.dumps({"zarr_format": 2}))
    for name, sep, val in (("a1", "/", 0x11), ("a2", ".", 0x22)):
        ad = os.path.join(root, "g", name)
        _w(os.path.join(ad, ".zarray"), json.dumps({
            "zarr_format": 2, "shape": [4, 4], "chunks": [2, 2], "dtype": "<i4",
            "fill_value": 0, "order": "C", "filters": None, "compressor": None,
            "dimension_separator": sep,
        }))
        for i in range(2):
            for j in range(2):
                b = bytes([val + i * 2 + j]) * 16
                if sep == "/":
                    _w(os.path.join(ad, str(i), str(j)), b)
                else:
                    _w(os.path.join(ad, f"{i}.{j}"), b)
    _w(os.path.join(root, "g", "a1", "notes.json"), '{"sidecar": true}')
    return root


def build_v3_sharded(root):
    _w(os.path.join(root, "zarr.json"), json.dumps({
        "zarr_format": 3, "node_type": "group", "attributes": {}}))
    _w(os.path.join(root, "a", "zarr.json"), json.dumps({
        "zarr_format": 3, "node_type": "array", "shape": [4, 4], "data_type": "int32",
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [2, 4]}},
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": 0,
        "codecs": [{"name": "sharding_indexed", "configuration": {
            "chunk_shape": [2, 2],
            "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
            "index_codecs": [{"name": "bytes", "configuration": {"endian": "little"}},
                             {"name": "crc32c"}],
            "index_location": "end"}}],
        "attributes": {},
    }))
    _w(os.path.join(root, "a", "c", "0", "0"), b"\xaa" * 64)
    _w(os.path.join(root, "a", "c", "1", "0"), b"\xbb" * 64)
    return root


# ---- frozen expectations (format tag zarrsum-id/1; see SPEC.md section 4) ----
VECTORS = {
    "v2_array": {
        "build": build_v2_array,
        "dataset_id": "1242a22abad11f2ecefe57942447d90ee6767e1f28a8cfd138e0a4157f5a6d85",
        "counts": {"n_stores": 1, "n_groups": 0, "n_arrays": 1, "n_files": 2, "n_nonzarr": 0},
        "stores": {".": "93687d92877769a7f9eb957e3c453bcbc733bb37cdf8c2717701db2bc2492beb"},
        "groups": {},
        "arrays": {".": "93687d92877769a7f9eb957e3c453bcbc733bb37cdf8c2717701db2bc2492beb"},
    },
    "v2_group": {
        "build": build_v2_group,
        "dataset_id": "4e53b4df8cfb0efe22d19c5c8a65f1ab3d30fbb324cfde6b28ba4f9dfc8b9ee2",
        "counts": {"n_stores": 1, "n_groups": 2, "n_arrays": 2, "n_files": 8, "n_nonzarr": 1},
        "stores": {".": "ab61367a234accced0fb2bf1ebd8bb46f4380b60b92579ecbeaac041546d16a0"},
        "groups": {
            ".": "ab61367a234accced0fb2bf1ebd8bb46f4380b60b92579ecbeaac041546d16a0",
            "g": "7e489c201f64db1b7f6b692edb73961f671a5fb0e2d12321d6fcd365e6309b6e",
        },
        "arrays": {
            "g/a1": "4680cde3082308b4cac7e994401f90354a037b964cc276768153fdbaf64136ce",
            "g/a2": "d7d4490a327f03c8cbe3da3d9c5281c2a14bab666dc79bfa5240c6c60865b936",
        },
    },
    "v3_sharded": {
        "build": build_v3_sharded,
        "dataset_id": "52fc6c614f7ce52c8ca4b5ca5bea1a1a2ba8daadaa1a13eef29a125a65cb79a6",
        "counts": {"n_stores": 1, "n_groups": 1, "n_arrays": 1, "n_files": 2, "n_nonzarr": 0},
        "stores": {".": "f743b30dfd415bbba4476e8d87aa96e36b3d69b19c31e908ad1a052171cf62e6"},
        "groups": {".": "f743b30dfd415bbba4476e8d87aa96e36b3d69b19c31e908ad1a052171cf62e6"},
        "arrays": {"a": "32ed38679be8b15c631c8a8bacb57bc1cc5b95307b2a110887ba99635bbbffc0"},
    },
}


@pytest.mark.parametrize("name", list(VECTORS))
def test_identity_vector(name):
    v = VECTORS[name]
    with tempfile.TemporaryDirectory() as base:
        root = v["build"](os.path.join(base, name))
        r = zarrsum.hash_dataset(root)
    assert r["format"] == "zarrsum-id/1"
    assert r["dataset_id"] == v["dataset_id"], f"{name}: dataset_id drifted"
    for k, want in v["counts"].items():
        assert r[k] == want, f"{name}: {k}={r[k]} want {want}"
    assert dict((rel, i) for rel, i in r["stores"]) == v["stores"], name
    assert dict((rel, i) for rel, i in r["groups"]) == v["groups"], name
    assert dict((rel, i) for rel, i, _n in r["arrays"]) == v["arrays"], name


@pytest.mark.parametrize("name", list(VECTORS))
def test_identity_vector_portable(name):
    """The frozen id must not depend on the storage path or the worker count."""
    v = VECTORS[name]
    with tempfile.TemporaryDirectory() as b1, tempfile.TemporaryDirectory() as b2:
        r1 = zarrsum.hash_dataset(v["build"](os.path.join(b1, "x")), max_workers=1)
        r2 = zarrsum.hash_dataset(v["build"](os.path.join(b2, "y")), max_workers=8)
    assert r1["dataset_id"] == r2["dataset_id"] == v["dataset_id"], name


if __name__ == "__main__":
    for _name in VECTORS:
        test_identity_vector(_name)
        test_identity_vector_portable(_name)
    print("PASS")
