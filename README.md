# zarrsum

A Zarr-aware content id for versioning Zarr datasets at scale.

`zarrsum` gives a Zarr store one portable id. The id does not depend on the node, the
file-enumeration order, file mtimes, or the chunk-key separator. A manifest can pin a
dataset by this id. The id is a BLAKE3 Merkle tree, so a new version re-hashes only what
changed, and the manifest can pin a whole store or a subset of its arrays.

## What the id is

- A **BLAKE3 Merkle tree**: chunk -> array -> group -> store, then a dataset id that
  combines the store ids with the non-Zarr files.
- **Two leaf policies.** `bytes` reads and hashes every payload byte. It requires a
  stable source. `inventory` records the size and mtime of each payload file. It still
  requires enumeration and file stats. It can miss an edit that preserves both fields.
  Storage corruption can also preserve them. Metadata and non-Zarr files are hashed
  in full under both policies. The formats differ: `zarrsum-id/1` and `zarrsum-inv/2`.
  Use the bytes policy for fresh content verification.
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

### Experimental pinned push retry

An interrupted push leaves its original `push.plan.json` and `push.submission.json` in
the selected evidence directory. Retry the same release, sender path, remote, and prefix.
Supply the original digest from the submission record. Do not edit the plan.

```bash
zarrsum-release push "$SENDER" "$RELEASE" --remote test --remotes remotes.json \
  --evidence evidence/retry --resume-plan evidence/first/push.plan.json \
  --plan-digest "$ORIGINAL_PLAN_BLAKE3"
```

Resume checks the plan identity against the complete source graph and destination.
It reads and verifies existing target content before reusing any key. It preserves the
original plan bytes and digest. Target readback and conditional root publication still run.
A corrupt existing object causes refusal. The remote owner must restore or remove the
identified corrupt object before retry. This command does not silently overwrite it.
The 10,000-entry child-plan limit remains. Current remote graph and scratch budgets
are specified below.

`--transfer-timeout SECONDS` bounds each rclone subprocess, including probes and copies.
Its default is 3600 seconds. Use an explicit smaller value for bounded failure tests.
An expired deadline stops and reaps the child process and fails the operation. It does
not mark a partial remote release complete. This bound is separate from rclone's HTTP
idle timeout and from the allocation time limit.

### Bounded multi-plan pushes

A push whose complete nonroot graph exceeds 10,000 entries now writes a version 2
index at `push.plan.json`. The index pins the release ID, absolute sender root,
destination, repository endpoint, ordered child hashes, and entry counts. Child plans
live under `plans/<digest>.json` beside the index. Each child retains the 10,000-entry
limit. The index permits at most 64 children and 64 KiB; each child permits at most
32 MiB. Current remote graph limits appear below. The index separates the graph count from
the per-child plan limit; it does not remove graph validation or scratch budgets.

The child union must cover the exact required nonroot graph. Missing, duplicate, foreign,
misordered, or changed entries fail validation before transfer. The root never enters a
bulk plan. Publication still requires fresh verification of the complete target graph.
An existing remote object must pass a fresh byte check before reuse.

Resume uses the same `--resume-plan` and `--plan-digest` options as a single-plan push.
Keep the index and its entire sibling `plans/` directory together. The digest identifies
the index; the index identifies every child. An indexed retry preserves those bytes and
skips content that passes the remote byte checks. Legacy version 1 plans remain supported.


### Prepare releases from candidate locations

The experimental `zarrsum-release prepare` command selects a verified local cache or
an accessible retained store. It accepts Hermes-shaped filesystem and S3 location records.
It returns verified ordinary files and a receipt for a future job-submission adapter.
See [LOCATIONS.md](LOCATIONS.md) for the reference format, access mapping, byte budget,
URI proposal, and integration limits. This does not deploy a Katamari submission hook.
### Bounded remote graph validation

The experimental remote graph reader now keeps expected membership, registry namespaces,
file records, object identities, metadata cache, and missing-object indexes in a private
SQLite scratch directory. It processes one manifest at a time. Registry validation and
path generation iterate registry lines instead of building full Python ID lists. The
release identity and object bytes do not change.

The current declared-file cap is 200,000. This is a finite supported-input bound, not a
performance claim. Each metadata or payload body retains its 32 MiB limit. Each graph
workspace has an 8 MiB SQLite page cache, disabled mmap, disk temporary storage, and a
2 GiB database limit. Decoded JSON and library memory are additional; RSS must be measured.
Keep TMPDIR on private scratch with sufficient space. Workspace files are disposable and
removed when the operation releases them. Host loss may require owned-scratch cleanup.

Push and large fetch construct ordered child plans incrementally. Each child stays at
10,000 entries; the index keeps its existing count and byte bounds. The validator checks
strict global key order, expected content, and exact total coverage without retaining a
whole-graph seen set. Fetch still admits only checked content and requires full graph
verification before a verified checkout. Pinned push retries preserve their existing rules.

Full subprocess commands and transfer results are recorded in `commands.jsonl` and
`transfers.jsonl` in the evidence directory. Each has a 64 MiB limit. CLI JSON retains at
most 256 preview records and includes full record counts. Do not treat the preview length
as the number of processes or transfers. Transfer and destination byte checks are unchanged.

### Native transfer batch size

`--transfer-batch-size N` selects 1 to 1,024 exact keys per native copy. The default
remains 64. It does not change child-plan bytes, release identity, rclone concurrency,
source/destination presence checks, target readback, or destination byte verification.
The direct backend also limits the encoded file list to 512 KiB before starting rclone.
Operation output records the selected batch size. For the tested small-file workflow,
select 1,024 keys explicitly on `push`, `fetch`, or `pull`:

```bash
zarrsum-release push "$SENDER" "$RELEASE" --remote test --remotes remotes.json \
  --evidence evidence/push --transfer-batch-size 1024
```

Use `--transfer-batch-size 64` to select the default size explicitly. The Bruno two-host
test on 2026-11-19 used 100,000 unique 1 KiB files and two trials per size. Mean verified
push time fell from 889 to 481 seconds. Mean cold pull time fell from 638 to 261 seconds.
The runtime used base `66a0ec4` with the batch-control changes published here.
The [test record](https://github.com/aliddell/data-versioning/blob/c5c9ab6/reference/2026-11-19-bruno-stall-diagnostic.md)
contains source hashes and complete evidence. These measurements do not establish
performance for other networks or file sizes.

## Experimental release references and submitted jobs

[REFERENCE-POC.md](REFERENCE-POC.md) describes six explicit registration strategies.
They separate full hashing, imported producer digests, version assertions, and sampling.
The new interface does not change the existing transfer verification defaults.

The optional `metadata_releases`, `catalog_releases`, and `submissions` modules support
owned integration tests. They preserve metadata revisions and exact job inputs.
Catalog tests use the pinned external client and an HTTP test transport. Submission
tests use a durable journal and refuse automatic resubmission after uncertain dispatch.
The examples name Reef fixture resources. Production deployment and a storage retention
agreement remain separate work. The optional catalog modules do not add dependencies
to the reference registration CLI.

## Experimental direct OZX output

`zarrsum-release pull --ozx RELEASE_ID DESTINATION --remote ALIAS --remotes CONFIG
--evidence DIRECTORY` writes a verified stored ZIP archive. It uses ZIP64 when needed.
The command reads the exact remote release and checks all declared payload bytes.
It creates no local payload cache or unpacked checkout. Existing output is preserved.
Incomplete downloads do not publish an archive. `--workers` selects one to eight SDK
read workers. Native `--transfer-batch-size` applies to directory transfers.

The graph limit remains 200,000 declared files. Each payload object remains limited to
32 MiB. The archive writer keeps a ZIP member index in memory. Metadata edits inside
an archive require a new archive. The tested archive-aware helpers can read OZX inputs.
The earlier Katamari v2 metadata builder and reader require separate archive-path work.
This command does not establish full Katamari OZX training support.

## Experimental shared reference workflow

`zarrsum-reference` publishes release documents to a shared filesystem, registers their
exact IDs with the Scientific Dataset Catalog, lists versions, freezes a resolution,
and prepares separate metadata with hard-linked payloads. See [REFERENCE-WORKFLOW.md](REFERENCE-WORKFLOW.md).
The catalog client and remote extra are required. Tests use an owned HTTP catalog
fixture. Filesystem preparation requires a mounted candidate on the checkout filesystem.
It does not add S3 payload recovery or managed Hermes submission to this reference format.

## Pin the native transfer executable

Use an explicit executable and an approved SHA-256 for reproducible native transfers:

```bash
zarrsum-release pull "$CACHE" "$RELEASE" "$CHECKOUT" \
  --remote test --remotes remotes.json --evidence evidence/pull \
  --rclone-binary "$TOOLS/rclone" --rclone-sha256 "$EXPECTED_RCLONE_SHA256"
```

The same flags apply to `push`, `fetch`, and `prepare`. Obtain the expected digest
from the approved tool package or deployment record. A mismatch refuses native
execution before SDK credential lookup or copying. The executable is resolved to
one absolute path and hashed once per operation. Later native calls refuse observed
changes to its device, inode, length, mode, or timestamps.

Native transfer receipts contain `transport.version` and `transport.binary`, including
`path`, `requested`, `sha256`, `expected_sha256`, and `pin_verified`. Preparation saves
the same record under `fetch.transport`. Existing `transport_version` output remains.
Without `--rclone-sha256`, the command records the observed digest and sets
`pin_verified` to false. The default remains compatible with existing invocations.

A complete-cache preparation does not load or execute rclone. Its empty fetch record
does not claim a checked native pin. Offline checkout also needs no executable.
`pull --ozx` uses the SDK only and refuses `--rclone-sha256`; its receipt does not
claim rclone provenance. Binary checks do not change release IDs or byte verification.
