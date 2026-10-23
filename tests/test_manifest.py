"""The manifest add/verify round-trip."""
import json
import os
import tempfile

import pytest

import zarrsum
from zarrsum import manifest


def _store(d, chunkbytes):
    os.makedirs(os.path.join(d, "a"))
    json.dump({"zarr_format": 2}, open(os.path.join(d, ".zgroup"), "w"))
    json.dump({"zarr_format": 2, "shape": [4, 4], "chunks": [2, 2], "dtype": "<i4",
               "fill_value": 0, "order": "C", "filters": None, "compressor": None,
               "dimension_separator": "."}, open(os.path.join(d, "a", ".zarray"), "w"))
    for key, b in chunkbytes.items():
        with open(os.path.join(d, "a", key), "wb") as f:
            f.write(b)
    return d


def test_add_then_verify_ok():
    with tempfile.TemporaryDirectory() as base:
        s = _store(os.path.join(base, "s"), {"0.0": b"\x01" * 16})
        mpath = os.path.join(base, "manifest.json")
        manifest.add_dataset(mpath, "ds", s, policy="bytes")
        assert manifest.verify_dataset(mpath, "ds")["ok"]
        # manifest is valid JSON with the entry
        m = json.load(open(mpath))
        assert m["datasets"]["ds"]["policy"] == "bytes" and m["datasets"]["ds"]["id"]


def test_verify_detects_change():
    with tempfile.TemporaryDirectory() as base:
        s = _store(os.path.join(base, "s"), {"0.0": b"\x01" * 16})
        mpath = os.path.join(base, "manifest.json")
        manifest.add_dataset(mpath, "ds", s, policy="bytes")
        with open(os.path.join(s, "a", "0.0"), "wb") as f:
            f.write(b"\x02" * 16)  # same size, new bytes
        assert not manifest.verify_dataset(mpath, "ds")["ok"]  # bytes policy catches it


def test_inventory_verify_sees_same_size_rewrite():
    with tempfile.TemporaryDirectory() as base:
        s = _store(os.path.join(base, "s"), {"0.0": b"\x01" * 16})
        mpath = os.path.join(base, "manifest.json")
        manifest.add_dataset(mpath, "ds", s, policy="inventory")
        with open(os.path.join(s, "a", "0.0"), "wb") as f:
            f.write(b"\x02" * 16)  # same size, new bytes -> mtime bumps
        assert not manifest.verify_dataset(mpath, "ds")["ok"]  # inventory catches it via mtime
        with open(os.path.join(s, "a", "0.0"), "wb") as f:
            f.write(b"\x02" * 32)  # different size
        assert not manifest.verify_dataset(mpath, "ds")["ok"]  # size change is caught too


def _nested(base):
    """A store with group 'g' holding two arrays, g/a1 and g/a2 (separator '/')."""
    d = os.path.join(base, "nested")
    json.dump({"zarr_format": 2}, open_mk(os.path.join(d, ".zgroup")))
    json.dump({"zarr_format": 2}, open_mk(os.path.join(d, "g", ".zgroup")))
    for a, val in (("a1", 1), ("a2", 2)):
        ad = os.path.join(d, "g", a)
        json.dump({"zarr_format": 2, "shape": [2, 2], "chunks": [2, 2], "dtype": "<i4",
                   "fill_value": 0, "order": "C", "filters": None, "compressor": None,
                   "dimension_separator": "/"}, open_mk(os.path.join(ad, ".zarray")))
        os.makedirs(os.path.join(ad, "0"))
        with open(os.path.join(ad, "0", "0"), "wb") as f:
            f.write(bytes([val]) * 16)
    return d


def open_mk(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, "w")


def test_subset_pin_is_real_and_localized():
    """A group/array pin verifies against that node only: a change to a sibling array leaves
    the pin valid, a change to the pinned array breaks it. This is what makes a subset pin
    meaningful rather than a relabelled whole-dataset pin."""
    with tempfile.TemporaryDirectory() as base:
        d = _nested(base)
        mpath = os.path.join(base, "manifest.json")
        # the pinned array id must equal the array id zarrsum reports for that node
        r = zarrsum.hash_dataset(d, policy="bytes")
        arr_ids = {rel: i for rel, i, _n in r["arrays"]}
        e = manifest.add_dataset(mpath, "arr", d, policy="bytes", unit="array", subpath="g/a1")
        assert e["id"] == arr_ids["g/a1"], "the entry must hold the array's own id"
        manifest.add_dataset(mpath, "grp", d, policy="bytes", unit="group", subpath="g")
        assert manifest.verify_dataset(mpath, "arr")["ok"]
        assert manifest.verify_dataset(mpath, "grp")["ok"]
        # change the sibling array a2: the a1 pin stays valid, the group pin breaks
        with open(os.path.join(d, "g", "a2", "0", "0"), "wb") as f:
            f.write(b"\x09" * 16)
        assert manifest.verify_dataset(mpath, "arr")["ok"], "a sibling change must not break a1"
        assert not manifest.verify_dataset(mpath, "grp")["ok"], "the group contains a2"
        # change the pinned array a1: now the a1 pin breaks
        with open(os.path.join(d, "g", "a1", "0", "0"), "wb") as f:
            f.write(b"\x09" * 16)
        assert not manifest.verify_dataset(mpath, "arr")["ok"]


def test_subset_pin_needs_a_valid_path():
    with tempfile.TemporaryDirectory() as base:
        d = _nested(base)
        mpath = os.path.join(base, "manifest.json")
        with pytest.raises(ValueError):
            manifest.add_dataset(mpath, "x", d, unit="array")  # no subpath
        with pytest.raises(KeyError):
            manifest.add_dataset(mpath, "x", d, unit="array", subpath="g/nope")


if __name__ == "__main__":
    test_add_then_verify_ok()
    test_verify_detects_change()
    test_inventory_verify_sees_same_size_rewrite()
    test_subset_pin_is_real_and_localized()
    test_subset_pin_needs_a_valid_path()
    print("PASS")
