# zarrsum

A Zarr-aware content id for versioning Zarr datasets at scale.

`zarrsum` gives a Zarr store one portable id. The id does not depend on the node, the
file-enumeration order, file mtimes, or the chunk-key separator. A manifest can pin a
dataset by this id. The id is a BLAKE3 Merkle tree, so a new version re-hashes only what
changed, and the manifest can pin a whole store or a subset of its arrays.

## What the id is

- A **BLAKE3 Merkle tree**: chunk -> array -> group -> store, then a dataset id that
  combines the store ids with the non-Zarr files.
- **Byte-exact** on chunk bytes. The hasher never decodes a chunk. A re-chunk or a
  re-compress changes the id; a separator or key-encoding change alone does not.
- **Zarr-aware**: it reads the key encoding from the array metadata, so it handles Zarr v2
  and the Zarr v3 `c/0/0/0` default. The leaf of the tree is a **data file**: for an
  unsharded array that is one chunk; for a sharded Zarr v3 array that is one shard (which
  holds many inner chunks). zarrsum hashes the file; it does not look inside a shard. The
  inner-chunk layout is still captured, because it lives in the array metadata, which is
  hashed. Non-Zarr files (for example sidecar JSON) are hashed on their own and combined at
  the end.

## Install

```
pip install -e .
```

The only runtime dependency is `blake3`. Python 3.10 or newer.

## Use

Command line:

```
zarrsum /path/to/store.zarr              # dataset id + per-store ids
zarrsum /path/to/dataset_dir --arrays    # also per-array ids (for subset pins)
zarrsum /path/to/store.zarr --json       # full result as JSON
```

Library:

```python
import zarrsum
r = zarrsum.hash_dataset("/path/to/store.zarr")
print(r["dataset_id"])
for relpath, array_id, n_files in r["arrays"]:
    print(relpath, array_id, n_files)
```

`hash_dataset` returns a dict with `dataset_id`, `stores`, `arrays`, and counts. All
values are JSON-safe.

## Test

```
python tests/test_semantics.py     # or: pytest
```

The tests check the two guarantees that matter: a separator change does not move the id,
and a re-chunk does.

## Status

Proof of concept. The core hasher works. It is validated on synthetic stores and on one
real Zarr v2 dataset: 856 GB across 152,359 chunk files in 9,035 arrays, in 71 stores,
with non-Zarr sidecar files interleaved (2026-10-19). Still to come: an incremental cache
keyed by `(relpath, size, mtime)` so a new version re-hashes in minutes, parallel reads,
and the DVC manifest integration.
