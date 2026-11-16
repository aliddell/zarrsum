# Computed-path release POC

This experimental interface publishes exact declared file sets. It computes chunk paths.
It does not scan the source directory. It retains a separate copy of each unique object's
bytes. Existing zarrsum commands and IDs do not change.

Run from the repository with Python 3.10 or later and the `blake3` dependency installed:

```
PYTHONPATH=src python -m zarrsum.releases --help
```

An installed package also provides `zarrsum-release`.

## Describe membership

Save this JSON as `descriptor.json`:

```json
{
  "version": 1,
  "files": [".zgroup", "image/.zarray", "normalization_stats.json"],
  "grids": [
    {"prefix": "image", "grid_shape": [2, 3], "separator": "."}
  ]
}
```

This includes the three explicit files and six chunks `image/0.0` through `image/1.2`.
`grid_shape` is the number of chunk or shard coordinates along each axis. It is not the
array shape in pixels. A separator of `/` generates `image/0/0` instead.

For sparse arrays, add `"present": [[0, 0], [1, 2]]` to include only those coordinates.
An empty presence list includes no chunks for that grid. Every declared file is required.
Missing files, unreadable files, and member symlinks fail publication. Unknown fields,
duplicate keys, unsafe paths, and overlapping generated keys are refused. Grid prefixes
must not contain each other. Scalar grids are outside the POC grammar.

The caller must supply a complete descriptor. The POC does not find omitted sidecars or
check Zarr validity. Extra files outside declared membership do not affect the release.

## Publish and resume

```
PYTHONPATH=src python -m zarrsum.releases publish retained source descriptor.json \
  --source-id snapshot-A --assume-immutable-source --workers 8
```

The output is JSON with `release_id`, file and byte counts, source-read counts, partition
reuse, and elapsed time. Source and retained store must be separate directories.

`--source-id` must identify a fixed source version. It is not a mutable dataset label.
The caller asserts that no source file changes during the operation or between resumed
attempts. The POC cannot create or enforce a snapshot. Per-open file checks detect some
concurrent edits. They do not establish a consistent filesystem snapshot.

Repeat the same command to resume completed partitions. The checkpoint is bound to the
descriptor, source path, source ID, parent ID, and change list. Resume verifies retained
bytes for completed partitions before reuse. It skips their source reads. It may redo an
unfinished partition. Use `--no-resume` for a full source read.

The local store permits one publisher. Workers use 64 deterministic partitions and stream
path plans to temporary files. At most one partition's records per worker remain in memory.
Each partition is limited to 100,000 members. The descriptor and sparse lists must fit in
memory. These are POC limits, not a claim of arbitrary dataset scale.

## Publish a known change

Save the complete changed-key list as `changes.json`, for example `[".zgroup"]`:

```
PYTHONPATH=src python -m zarrsum.releases publish retained source descriptor.json \
  --source-id snapshot-B --assume-immutable-source --parent RELEASE_A \
  --changes changes.json --assume-complete-changes
```

The POC opens only changed source files. It reuses other records and partitions from the
parent. Membership must remain the same for delta publication. A membership change requires
a full publication with a new descriptor. The normal path still generates all declared
keys and writes the temporary plan. Delta planning is proportional to total key count.

The change list is a producer assertion. It is not change detection. If it omits a changed
file, the release selects the retained parent bytes for that key. It does not identify the
current source. Fresh verification against the source can detect that mismatch.

## Verify and restore

```
PYTHONPATH=src python -m zarrsum.releases verify retained RELEASE_A
PYTHONPATH=src python -m zarrsum.releases restore retained RELEASE_A restored
PYTHONPATH=src python -m zarrsum.releases verify retained RELEASE_A --destination restored
```

Verification reads every declared byte. It checks manifest identities, lengths, and content
digests. Destination verification checks declared members only. It does not list or reject
unrelated files. Restore requires a new destination. It checks each retained object during
the copy. A failed restore removes its temporary output. It does not replace existing data.

Restore creates ordinary files with generated paths. It does not preserve permissions,
timestamps, ownership, or hardlinks. Protect the retained store from modification or deletion.
The store is not an independent backup merely because it has a different directory path.

## Format and boundaries

`computed-release/1` is experimental. It is separate from `zarrsum-id/1` and can change
during POC development. Pin the code revision and descriptor with a run record.

Objects use BLAKE3 over raw file bytes. Partition records contain `[key, length, digest]`.
Canonical JSON uses sorted keys, compact separators, and ASCII escapes. Paths use POSIX
separators. Files and grid descriptions are normalized before path generation. Each key
selects partition `int(BLAKE3(key UTF-8).hexdigest()[:8], 16) % 64`. Records preserve the
deterministic generation order within each partition. The root includes the descriptor
and ordered `[partition, digest, file_count, byte_count]` records. Its BLAKE3 is the release
ID. Source paths, source IDs, and timestamps do not enter content identity.

Source bytes are hashed while they are copied to temporary retained objects. Publication
does not perform a fresh destination read of every new object. Run `verify` for that check.
An existing content object is checked before deduplication. Parent reuse trusts retained
objects until fresh verification. Missing or corrupted retained data makes recovery fail.

Files are flushed before installation. Completed partition checkpoints and release roots
use local atomic file operations and directory flushes. Only completed roots appear under
`releases/`. Failed publication can leave retained objects and checkpoints. There is no
garbage collection. Process interruption is tested. Power-loss and NFS-server crash
durability are not established. Restore publication assumes no concurrent writer at its
destination. The retained store is a trusted local directory.

The POC has no storage adapter, remote transfer, packing, snapshot creation, branch merge,
automatic change detection, or distributed scheduler. A first full publication still opens
and reads every source member. An exploded restore still creates every output file.
