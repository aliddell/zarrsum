"""Semantic guarantees of the id (design section 2).

Run with pytest, or directly: ``python tests/test_semantics.py``.
"""
import json
import os
import tempfile

import zarrsum


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


if __name__ == "__main__":
    test_separator_independent_and_rechunk_sensitive()
    test_deterministic()
    print("PASS")
