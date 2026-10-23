# The zarrsum content-id format

This document fixes the byte-level construction of a zarrsum content id. It is the identity
contract: an id names a dataset, and any implementation that follows this document computes
the same id for the same bytes. The reference implementation is `src/zarrsum/core.py`. The
frozen test vectors in `tests/test_identity.py` are the executable form of this document; they
fail if the format changes.

Write all numbers in this document as the code writes them. All lengths are unsigned 64-bit
little-endian. All hashes are BLAKE3, 32 bytes. Text is UTF-8.

## 1. Primitives

### 1.1 The tagged frame `H(tag, items)`

Every interior node and every serialized list uses one framing function. `items` is an ordered
list of byte strings.

    H(tag, items):
        h = BLAKE3()
        h.update( u64le(len(tag)) ); h.update(tag)
        for item in items:
            h.update( u64le(len(item)) ); h.update(item)
        return h.digest()          # 32 bytes

The length prefix on the tag and on every item makes the framing unambiguous: no concatenation
of two different item lists can produce the same input.

### 1.2 The data-file leaf `L(file)`

A data file is one chunk (unsharded array) or one shard (sharded Zarr v3 array). A shard is
**never opened**; it is hashed as opaque bytes.

- Policy `bytes` (format tag `zarrsum-id/1`): `L = BLAKE3(raw file bytes)`. This is a plain
  BLAKE3 of the file content, **not** wrapped in `H`.
- Policy `inventory` (format tag `zarrsum-inv/2`): `L = H("size+mtime", [u64le(st_size),
  u64le(st_mtime_ns)])`. No bytes are read. `st_mtime_ns` makes an id depend on the file mtime,
  so an `inventory` id is a change signal on one filesystem, not a portable identity (a copy
  resets mtimes). The vectors below are for `bytes` only, because they must be reproducible.

A non-Zarr file (a sidecar, a log) is always hashed as `BLAKE3(raw file bytes)` under both
policies.

### 1.3 Metadata canonicalization `C(bytes)`

    C(b, path):
        try obj = json.parse(b)
        except: raise DatasetMetadataError(path)  # invalid metadata JSON fails; a release is not
                                                  # identified over an empty placeholder
        if obj is a JSON object:
            drop the keys "dimension_separator" and "chunk_key_encoding"
        return json.dump(obj, sort_keys=True, separators=(",",":")).utf8

So the on-disk formatting of a metadata file (whitespace, key order) never affects the id;
only the parsed content does. Two keys are dropped before hashing, so a change to the chunk-key
separator or the chunk-key encoding alone does not change the id (the chunk **keys** are
normalized separately; see 2.1).

A metadata item for a file named `m` is `m.utf8 ++ b"=" ++ C(content)`.

## 2. The tree

### 2.1 Canonical data-file key

Within an array directory, a data file's path relative to the array root maps to a canonical
key: the integer chunk/shard coordinates joined by `"."`, for example `"0.0.0"`. The mapping
reads the chunk-key encoding from the array metadata:

- **Zarr v2.** Separator is `dimension_separator` (default `"."`). With `"/"`, split the
  relative path on the OS separator; otherwise split on the separator character. A path that
  contains the OS separator but declares a non-`"/"` separator is **not** a data file.
- **Zarr v3.** `chunk_key_encoding.name == "default"` uses the `c` prefix and default separator
  `"/"`; `"v2"` uses no prefix and default separator `"."`. The separator may be overridden in
  `configuration.separator`. A `"default"` file is `c/0/0`; the `c` prefix is stripped.

Every coordinate part must be all digits. A file that does not parse to a key is a non-Zarr
file. Because the key is normalized, the same logical chunks give the same array id whether they
are stored as `0/0` or `0.0` (separator independence). A re-chunk, re-shard, or re-compression
changes the file bytes and so changes the id.

### 2.2 Array node

    leaves   = sorted list of (key, L(file)) over the array's data files, by key
    leaf_ser = H("files", [ key.utf8 ++ b"\x00" ++ L  for each leaf ])
    meta     = [ metadata item for m  for m in (".zarray","zarr.json",".zattrs") if present ]
    array_id = H("array", meta ++ [leaf_ser])

### 2.3 Group node

    children  = child nodes, sorted by name; each is an array (2.2) or a group (2.3)
    child_ser = H("children", [ name.utf8 ++ b"\x00" ++ child_id  for each child ])
    meta      = [ metadata item for m  for m in (".zgroup","zarr.json",".zattrs") if present ]
    group_id  = H("group", meta ++ [child_ser])

### 2.4 Dataset

A dataset root may hold one or more stores (a directory with a `.zgroup`, `.zarray`, or
`zarr.json` marker) plus non-Zarr files. `rel` is a path relative to the dataset root; a store
that is the root itself has `rel == "."`.

    store_ser  = H("stores",  [ rel.utf8 ++ b"\x00" ++ store_id   for each store, sorted by rel ])
    nz_ser     = H("nonzarr", [ rel.utf8 ++ b"\x00" ++ L(file)    for each non-Zarr file, sorted by rel ])
    dataset_id = H("dataset", [ format_tag.utf8, store_ser, nz_ser ])

`format_tag` is `zarrsum-id/1` (bytes) or `zarrsum-inv/2` (inventory). The tag is inside the
root hash, so a change of policy gives a different id and a reader always knows which policy
made an id.

## 3. What is in and out of the id

- **Excluded: `.zmetadata`.** Consolidated metadata is not hashed. Adding or removing it does
  not change the id. The authoritative metadata for hashing is the per-node metadata files.
  **Read policy for a consumer:** two stores that share an id can still hold different
  `.zmetadata`, so a consumer must not treat consolidated metadata as part of the identity. After
  a `verify`, read the per-node metadata, or regenerate `.zmetadata` from it. Pinning the
  consolidated bytes themselves would need a separate exact-release identity (not built).
- **Excluded: symlinks.** A symlink to a file or a directory is skipped entirely. The id covers
  the real files in the tree, not symlink targets.
- **Excluded: ignored directories.** Directory names matching the ignore globs (default
  `.trash*`, `*_temp`, `*_temp_*`, `*.tmp`) are dropped before hashing.
- **Not in the id:** the storage path, file permissions, the file order on disk, and the worker
  count. A `bytes` id is identical across nodes, copies, and worker counts.
- **In the id:** every data-file's content (under `bytes`), the per-node Zarr metadata (after
  canonicalization), the tree structure, the canonical chunk keys, and every non-Zarr file's
  content.

A `bytes` id is a normalized fingerprint of the stored content — the exact chunk and shard bytes
and the canonicalized per-node metadata — not a byte-for-byte image of the directory, and not the
logical array alone. A layout change (re-chunk, re-shard, re-compress) changes it. Invalid
metadata JSON is refused (`DatasetMetadataError`), so an id is never computed over a placeholder.

## 4. Test vectors

The three fixtures below are built byte-for-byte by `tests/test_identity.py` and hashed with the
`bytes` policy. The expected `dataset_id` is frozen; the test fails if the format drifts. Each
JSON metadata file is written as compact JSON, but recall from 1.3 that the formatting does not
affect the id.

### Vector 1 — `v2_array` (a single Zarr v2 array as the whole store)

    .zarray   {"zarr_format":2,"shape":[4],"chunks":[2],"dtype":"<i4","fill_value":0,
               "order":"C","filters":null,"compressor":null,"dimension_separator":"."}
    .zattrs   {"scale":[1,2]}
    0         bytes 0x00..0x07   (8 bytes)
    1         bytes 0x08..0x0f   (8 bytes)

    dataset_id = 1242a22abad11f2ecefe57942447d90ee6767e1f28a8cfd138e0a4157f5a6d85
    store "."  = 93687d92877769a7f9eb957e3c453bcbc733bb37cdf8c2717701db2bc2492beb
    (n_stores=1, n_groups=0, n_arrays=1, n_files=2, n_nonzarr=0)

### Vector 2 — `v2_group` (root group, subgroup `g`, arrays `g/a1` and `g/a2`, a sidecar)

`g/a1` uses the `"/"` separator; `g/a2` uses `"."`. Each array has shape `[4,4]`, chunks
`[2,2]`, so four chunks `0.0 0.1 1.0 1.1`. A chunk of `a1` is `bytes([0x11+2i+j])*16`; of `a2`,
`bytes([0x22+2i+j])*16`. `g/a1/notes.json` holds `{"sidecar": true}` and is a non-Zarr file.

    dataset_id   = 4e53b4df8cfb0efe22d19c5c8a65f1ab3d30fbb324cfde6b28ba4f9dfc8b9ee2
    group "."    = ab61367a234accced0fb2bf1ebd8bb46f4380b60b92579ecbeaac041546d16a0
    group "g"    = 7e489c201f64db1b7f6b692edb73961f671a5fb0e2d12321d6fcd365e6309b6e
    array "g/a1" = 4680cde3082308b4cac7e994401f90354a037b964cc276768153fdbaf64136ce
    array "g/a2" = d7d4490a327f03c8cbe3da3d9c5281c2a14bab666dc79bfa5240c6c60865b936
    (n_stores=1, n_groups=2, n_arrays=2, n_files=8, n_nonzarr=1)

### Vector 3 — `v3_sharded` (a Zarr v3 group and one sharded array `a`)

The array uses the `sharding_indexed` codec with the default chunk-key encoding. There are two
shard files `a/c/0/0` and `a/c/1/0`, of `0xaa*64` and `0xbb*64`. zarrsum never decodes a shard,
so the bytes are arbitrary.

    dataset_id = 52fc6c614f7ce52c8ca4b5ca5bea1a1a2ba8daadaa1a13eef29a125a65cb79a6
    group "."  = f743b30dfd415bbba4476e8d87aa96e36b3d69b19c31e908ad1a052171cf62e6
    array "a"  = 32ed38679be8b15c631c8a8bacb57bc1cc5b95307b2a110887ba99635bbbffc0
    (n_stores=1, n_groups=1, n_arrays=1, n_files=2, n_nonzarr=0)
