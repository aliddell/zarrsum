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

The byte-level format is frozen in [`SPEC.md`](SPEC.md), with test vectors in
`tests/test_identity.py`. A second implementation that follows `SPEC.md` computes the same id.

## Install

```
pip install -e .
```

The only runtime dependency is `blake3`. Python 3.10 or newer.

## Use

Command line:

```
zarrsum /path/to/store.zarr                    # dataset id + per-store ids (bytes policy)
zarrsum /path/to/store.zarr --policy inventory # (size, mtime) id, no byte reads
zarrsum /path/to/dataset_dir --arrays --groups # also per-array and per-group ids (subset pins)
zarrsum /path/to/store.zarr --json             # full result as JSON
zarrsum /path/to/store.zarr --workers 1        # serial (default: auto-parallel)
zarrsum /path/to/store.zarr --fence            # abort if a file changes mid-hash
zarrsum /path/to/store.zarr --min-quiet 60     # refuse if the newest mtime is < 60 s old
```

The id itself assumes a quiescent store; it is not defined for a store being written. Two
optional guards help enforce that: `--min-quiet SECONDS` refuses to start while the newest
file was touched inside the window, and `--fence` re-stats every file after hashing and
aborts if any changed during the run (an in-place edit, an append, or a metadata rewrite). A
file created after the crawl is not seen by either; neither is a substitute for quiescing
writes.

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

To pin a subset instead of the whole store, name the node with `--unit` and `--path` (the
path a group or array has under `zarrsum --groups`/`--arrays`). `verify` then recomputes and
compares that node alone, so a change to a sibling array leaves the pin valid:

```
zarrsum-manifest add manifest.json site_a /path/to/data --unit array --path g/a1
zarrsum-manifest verify manifest.json site_a                 # compares the g/a1 array id
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
## Computed-path release experiment

See [RELEASES.md](RELEASES.md) for the experimental release interface. It publishes from
declared paths and retains bytes for recovery. Its release IDs are separate from the
existing normalized zarrsum IDs.

## Experimental direct-rclone release remotes

Install `pip install '.[remote]'`. Install rclone separately. Hermes is not required.
This bounded prototype accepts exact release IDs. It retains the existing release format.
The current graph limit is 10,000 files. The current object limit is 32 MiB.
It is not a production retention service.

Keep the SDK credentials in a private AWS profile file. Keep matching credentials in a
private rclone S3 config. The config must set an explicit endpoint, region, `env_auth=false`,
and `force_path_style`. The SDK profile and the named rclone remote must use matching
credentials. The release config stores profile and config-file references only.
Use HTTPS for a public endpoint. HTTP is restricted to numeric private addresses.
The compute test adds an exact allocated-endpoint check in its harness.

For a prepared sender store, use this sequence. Set the paths and release ID first.
The `fixture` profile and `payload` rclone remote refer to private credential files.
Removing the alias does not remove data.

```bash
zarrsum-release remote --config remotes.json add test \
  --endpoint http://10.1.2.3:13900 --bucket release-test --prefix retained \
  --region garage --addressing-style path --profile fixture \
  --rclone-remote payload --rclone-config /private/rclone.conf
zarrsum-release remote --config remotes.json list
zarrsum-release push "$SENDER" "$RELEASE" --remote test --remotes remotes.json --evidence evidence/push
zarrsum-release fetch "$CACHE" "$RELEASE" --remote test --remotes remotes.json --evidence evidence/fetch
zarrsum-release checkout "$CACHE" "$RELEASE" "$CHECKOUT" --evidence evidence/checkout
zarrsum-release verify "$CACHE" "$RELEASE" --destination "$CHECKOUT"
zarrsum-release pull "$CACHE" "$RELEASE" "$SECOND_CHECKOUT" --remote test --remotes remotes.json --evidence evidence/pull
zarrsum-release remote --config remotes.json remove test
```

`--rclone-binary` and `--rclone-config` select explicit transport settings for each remote
operation. `--bwlimit` supports bounded test transfers. Fetch creates no working checkout.
Pull fetches, constructs a private checkout, and freshly verifies it before exposure.
Checkout and destination verify work with the remote server stopped. `restore` remains
available with its existing behavior. An existing destination is preserved.

The backend copies bounded exact-key batches. It checks source presence before copying.
It does not expand a missing file into a directory. It always copies each selected key.
The release layer separately checks BLAKE3, complete graph membership, existing cache
bytes, and destination bytes. Transfer success is not a verification receipt.
SIGTERM cancels a CLI transfer and cleans its child processes and private temporary area.
An external SIGKILL or host loss cannot run process cleanup. A new attempt still requires
a complete byte check before success. Partial-download reuse is not implemented.
