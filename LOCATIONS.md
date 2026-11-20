# Prepare an identified release from candidate locations

This experimental interface resolves a retained release into verified local files.
The release ID identifies content. Locations tell the preparer where copies may exist.
A location change does not change the release ID.

Install the declared remote dependencies with `pip install '.[remote]'`.
No Hermes package, service, DynamoDB table, or Catalog lookup is required.

## Release reference

```json
{
  "version": 1,
  "release_id": "<64-character content digest>",
  "locations": [
    {
      "storage_platform": "reef",
      "location_uri": "reef://publisher/release-store"
    },
    {
      "storage_platform": "sf_hpc",
      "location_uri": "sf_hpc:///hpc/mydata/publisher/release-store"
    },
    {
      "storage_platform": "s3",
      "location_uri": "s3://example-bucket/publisher/release-store"
    }
  ]
}
```

Each location uses Hermes's `storage_platform` and `location_uri` field names.
The outer document is the experimental release-reference format. Its locations name
retained release-store roots, not mutable source datasets or individual Catalog assets.
The root record for the selected ID must exist inside the chosen store.

The document is limited to 64 KiB and 16 candidate locations. Duplicate JSON fields,
unknown fields, duplicate locations, invalid IDs, platform/scheme mismatches, URI
credentials, and unsupported schemes are rejected. An empty list permits cache-only use.

This implementation supports `reef`, `sf_hpc`, and `s3`. The caller explicitly declares
which filesystem platform is local. A location on another platform is skipped without
attempting to read its filesystem. These schemes do not establish SSH connections.
Direct Bruno-to-Reef transfer still needs a supported route or a separately staged copy.

Reef URI interpretation uses the approved projects-root policy:

- `reef://dataset/store` maps under `/mnt/main0/projects/dataset/store`.
- Three-slash absolute Reef forms are not supported.
- The resolved path must remain under `/mnt/main0/projects`, including symlink resolution.
- Keep home and scratch paths in job-local configuration.

HPC store URIs must contain absolute paths. Store format and content validation are
independent of the URI scheme. The current implementation needs fanout-2 caches and
uses the combined bounded remote graph validator. Its declared-file limit is 200,000.
Graph membership and missing-content bookkeeping use disposable SQLite scratch. Each
workspace retains an 8 MiB page cache and a 2 GiB database bound. Metadata bodies remain
bounded at 32 MiB. Set TMPDIR to private scratch with sufficient free space.

## Prepare before starting the loader

```sh
zarrsum-release prepare release.json /path/to/owned-cache /path/to/new-checkout \
  --local-platform reef --evidence /path/to/new-evidence
```

The command first checks the complete cache with fresh hashes. A valid cache needs no
remote, profile, rclone process, or provider connection. If content is missing, candidates
are tried in the supplied order. Unavailable, incomplete, and corrupt candidates can be
refused before trying the next one. Available cached objects are freshly verified before
reuse. A corrupt cache requires explicit owner repair; it is not silently overwritten.

For a filesystem source, missing objects first enter a private staging directory. Content
is checked before cache admission. The release root enters the cache only after all
required nonroot content is present and freshly checked. A failed attempt exposes no
checkout. A verified partial cache can be reused on retry. Source reads remain read-only.
Caches must be owned, cooperative stores with one preparer writing at a time.

The default budget for new content per candidate is 2 GiB. Use `--max-fetch-bytes` to
set another explicit byte budget. Zero permits existing content only. This budget does
not include existing cache bytes, the checkout copy, metadata probes, or HTTP overhead.
The caller must also budget those resources. Existing graph/body/child-plan bounds remain.

The checkout is built privately and freshly verified before it is exposed at the requested
path. An existing destination, including a dangling symlink, is refused before cache
access. Caller data is preserved. Each successful preparation needs a new evidence directory.

## S3 access configuration

Credentials stay in private SDK and rclone profiles. Existing named remotes supply the
endpoint, bucket, prefix, region, addressing mode, and profile/config references.
A separate local JSON file maps the exact S3 location to its named remote:

```json
{
  "s3://example-bucket/publisher/release-store": "publisher-s3"
}
```

```sh
zarrsum-release prepare release.json /path/to/owned-cache /path/to/new-checkout \
  --local-platform reef --evidence /path/to/new-evidence \
  --remotes remotes.json --location-remotes location-remotes.json \
  --rclone-binary rclone --transfer-timeout 3600
```

The alias must match the exact bucket and prefix. Existing connection validation requires
the SDK and rclone endpoint, region, addressing mode, and credentials to agree. The S3
adapter reuses existing graph validation, exact-key transfer, verified admission, and
fresh checkout checks. It performs metadata preflight to enforce the new-content budget.
This adds metadata reads; it is not a transfer optimization.

One S3 location maps to one configured provider in a preparation. The URI does not encode
a custom S3 endpoint. Configure that endpoint through the named access profile. Cross-provider
fallback for identical bucket/prefix spellings is outside this initial contract.

## Submission adapter contract

Python callers can use `zarrsum.locations.prepare(...)`. It returns the receipt below.
The CLI prints the release ID, destination, receipt path, and `result: verified` only on
success. A submission adapter must stop when preparation raises or the CLI exits nonzero.
SIGTERM and keyboard interruption return exit 130 in the CLI.

The success receipt is `prepare.receipt.json`. It records:

- The content ID, normalized release reference, and its document digest.
- The selected location, or a cache hit with no provider contact.
- The resolved filesystem root or non-secret S3 access alias and repository details.
- Cache and destination paths, declared local platform, new-content byte budget,
  attempt outcomes, and operation timings.
- Fresh destination-verification results, event time, and resolver source hash.

`resolution.json` records selection attempts even on failure. Candidate diagnostics use
fixed messages and do not dump provider errors or credential values. S3 attempts retain
the existing transfer evidence in their own directories. The reference and receipt contain
no credential values. A receipt records what was checked; it is not a signed attestation.

Run preparation where the target filesystem is accessible, before Katamari's manifest
checks and loader construction. Use the returned destination to map image, manifest,
centroid, and normalization inputs. Save the release ID and receipt with the job record.
The existing loader continues to receive ordinary local paths.

This interface does not yet patch production Katamari or Damacy submission. The earlier
bounded launcher adapter demonstrates the path mapping on HPA inputs. Multi-node preparation,
cache concurrency, access authorization, retention ownership, and production orchestration
need separate integration. A publisher must preserve at least one accessible retained copy.


## Native transfer batch size

`prepare --transfer-batch-size 1024` selects the tested larger small-file batch.
Python callers pass `transfer_batch_size=1024`. The default stays 64. Values must
be integers from 1 through 1024. The selected value reaches the S3 transfer
boundary and appears in the preparation receipt. The 512 KiB encoded-list bound,
plan identity, cache admission, and fresh byte checks remain unchanged.

Portable Reef references use the projects-relative form only. Job-local cache,
checkout, and evidence paths remain ordinary local configuration.
