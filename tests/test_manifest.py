"""The manifest add/verify round-trip."""
import json
import os
import tempfile

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


def test_inventory_verify_blind_to_same_size():
    with tempfile.TemporaryDirectory() as base:
        s = _store(os.path.join(base, "s"), {"0.0": b"\x01" * 16})
        mpath = os.path.join(base, "manifest.json")
        manifest.add_dataset(mpath, "ds", s, policy="inventory")
        with open(os.path.join(s, "a", "0.0"), "wb") as f:
            f.write(b"\x02" * 16)  # same size
        assert manifest.verify_dataset(mpath, "ds")["ok"]  # inventory is blind (by design)
        with open(os.path.join(s, "a", "0.0"), "wb") as f:
            f.write(b"\x02" * 32)  # different size
        assert not manifest.verify_dataset(mpath, "ds")["ok"]  # size change is caught


if __name__ == "__main__":
    test_add_then_verify_ok()
    test_verify_detects_change()
    test_inventory_verify_blind_to_same_size()
    print("PASS")
