# zarrsum

A Zarr-aware content id for versioning Zarr datasets at scale.

`zarrsum` gives a Zarr store one portable id. The id does not depend on the node, the
file-enumeration order, file mtimes, or the chunk-key separator. A manifest can pin a
dataset by this id. The id is a BLAKE3 Merkle tree, so a new version re-hashes only what
changed, and the manifest can pin a whole store or a subset of its arrays.

## What the id is

- A **BLAKE3 Merkle tree**: chunk -> array -> group -> store, then a dataset id that
  combines the store ids with the non-Zarr files.
- **Two leaf policies.** `bytes` (default) hashes every data-file's bytes — always sound,
  but it reads everything. `inventory` hashes a data-file's **`(size, mtime)`** (a stat, no
  byte read) — near-free at PB scale. An in-place rewrite bumps the mtime, so it is seen; the
  id is blind only to a rewrite that keeps *both* the size and the mtime (which needs the
  mtime deliberately preserved). Metadata and non-Zarr files are always hashed in full under
  both policies, so a metadata edit is always caught. The policy is named in the id's format
  version (`zarrsum-id/1` vs `zarrsum-inv/2`), so the two never look alike. For transfer or
  corruption QC (bit-rot keeps the same size and mtime), use the `bytes` policy.
- **Zarr-aware**: it reads the key encoding from the array metadata, so it handles Zarr v2
  and the Zarr v3 `c/0/0/0` default. The leaf of the tree is a **data file**: for an
  unsharded array that is one chunk; for a sharded Zarr v3 array that is one shard (which
  holds many inner chunks). zarrsum hashes the file; it does not look inside a shard. The
  inner-chunk layout is still captured, because it lives in the array metadata, which is
  hashed. Non-Zarr files (for example sidecar JSON) are hashed on their own and combined at
  the end.
- Under either policy, a re-chunk, a re-shard, or a re-compress changes the id (the metadata
  and the file inventory change); a `dimension_separator` or `chunk_key_encoding` change
  alone does not.

## Install

```
pip install -e .
```

The only runtime dependency is `blake3`. Python 3.10 or newer.

## Use

Command line:

```
zarrsum /path/to/store.zarr                    # dataset id + per-store ids (bytes policy)
zarrsum /path/to/store.zarr --policy inventory # size-only id, no byte reads
zarrsum /path/to/dataset_dir --arrays --groups # also per-array and per-group ids (subset pins)
zarrsum /path/to/store.zarr --json             # full result as JSON
zarrsum /path/to/store.zarr --workers 1        # serial (default: auto-parallel)
```

Library:

```python
import zarrsum
r = zarrsum.hash_dataset("/path/to/store.zarr", policy="inventory")
print(r["dataset_id"])
for relpath, array_id, n_files in r["arrays"]:
    print(relpath, array_id, n_files)
```

`hash_dataset` returns a dict with `dataset_id`, `format`, `policy`, `stores`, `groups`,
`arrays`, and counts. All values are JSON-safe.

## Manifest and DVC

A storage tool moves the bytes; zarrsum records and checks the identity. The manifest is a
small JSON file (keep it in git) that pins each dataset by its zarrsum id.

```
zarrsum-manifest add manifest.json training_data /path/to/data --policy inventory
zarrsum-manifest verify manifest.json training_data          # exit 0 if it matches its pin
```

A typical flow with DVC (which stores and transfers the data):

```
dvc add data                                             # DVC: one pointer in git + the bytes
zarrsum-manifest add manifest.json training_data data    # pin the identity
git add -A && git commit -m "track + pin" && dvc push    # bytes -> remote (S3, Reef, ...)
# elsewhere: a fresh clone
dvc pull                                                  # bytes <- remote
zarrsum-manifest verify manifest.json training_data      # prove you got the exact dataset
```

The verify step is what the manifest adds over DVC alone: a portable, Zarr-aware check that
the bytes you fetched are the exact dataset the manifest names. See `examples/dvc_demo.sh`
for a runnable end-to-end demo (including a tamper check).

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
