# Register a release without a full payload scan

This is an experimental interface. It is separate from the existing release store.
It does not change the default full-byte checks in the transfer commands.
Use `python -m zarrsum.revision_refs --help` to see its commands.

A release document declares every member. It keeps text metadata in the document.
Each payload entry declares its relative path and length. Binary metadata can be a
payload entry. The current bound is 10,000 physical members and a 16 MiB descriptor.
This is a bound for this experiment. It does not establish an organizational policy.
The tool does not search for undeclared files.

## Choose the evidence

| Mode | Payload work during registration | Basis and limit |
|---|---|---|
| `full` | Read and hash every declared payload byte. | Fresh BLAKE3 digests. The source must remain stable across the operation. |
| `producer-digest` | No payload access. | Trust full BLAKE3 digests from the producer. Record who supplied them. |
| `immutable-reference` | No payload access. | Trust the exact retained version named by the publisher. The tool does not verify the provider contract. |
| `producer-revision` | No payload access. | Trust the producer's scoped revision and membership. The producer must not change that revision. |
| `header-footer` | Read up to 64 KiB at each end of each file. | Fixed fingerprint. Changes outside these ranges are missed. |
| `random-blocks` | Read up to eight distinct 64 KiB blocks per file. | Sampled fingerprint. Record the seed and exact ranges. Unsampled changes are missed. |

The release ID is a SHA-256 digest of the document. It identifies the complete
assertions and metadata. It is not a digest of all dataset bytes. Full payload digests
use BLAKE3. Sampled fingerprints never supply deduplication keys.

A producer can compute full digests while it writes its output. Registration can then
import those digests. This removes a separate registration scan. It does not remove
the cost of hashing during production or the need to retain the exact bytes.

## Register A

Install this candidate package in an owned environment. It needs Python 3.10 or newer
and `blake3`. The reference interface does not need Katamari, the catalog client, or AWS.
Save this descriptor as `descriptor.json`:

```json
{
  "dataset": "example",
  "authority": "team:example-producer",
  "revision": "metadata-A",
  "payload_revision": "producer-output-17",
  "payloads": [{"path": "data.bin", "size": 1024}],
  "metadata": {"zarr.json": "{\"label\":\"A\"}"}
}
```

The following command reads only the descriptor. It does not require the payload source
to be available. Save its JSON receipt and `release_id` with the run record.

```sh
python -m zarrsum.revision_refs register descriptor.json \
  --mode producer-revision --output A.release.json > A.registration.json
```

Use `--mode full --root /path/to/stable/source` for fresh full digests.
For `producer-digest`, add the full `blake3` digest to every payload entry.
For either sampled mode, supply `--root`. Random sampling records its generated seed.
An optional `--seed` makes an experiment repeatable. A known seed is not a defense
against a writer that can choose where to change bytes.

## Select one retained source

Save this binding as `A.binding.json`. Replace `PINNED_ID` with the recorded release ID.
The binding is outside the release identity. It supplies one accessible filesystem root.

```json
{
  "release_id": "PINNED_ID",
  "authority": "team:example-producer",
  "payload_revision": "producer-output-17",
  "root": "/path/to/retained/output-17",
  "retention_assertion": "Describe the storage owner and exact retention agreement here."
}
```

```sh
python -m zarrsum.revision_refs bind A.release.json A.binding.json \
  --release-id PINNED_ID --verification reference-only > A.binding-receipt.json
```

`reference-only` opens each declared regular file and checks its length. It reads no
payload bytes. It trusts the root-to-revision mapping supplied by the operator.
It cannot detect a same-length rewrite. Receipts report this limitation.

`full` verification requires recorded full digests. `fingerprint` verification requires
recorded sample ranges. Neither policy is selected automatically. A request for full
verification of a revision assertion or a sampled release is refused.

Missing sources and changed authority or revision fields cause refusal. There is no
fallback to the producer path or latest release. Member symlinks and path escapes are
refused. The selected root remains a trust boundary. These checks do not authenticate
a storage provider or prevent mutation after binding.

The current binding accepts filesystem roots. The existing Hermes-compatible location
resolver remains separate. A Bruno snapshot must be accessible on the job host before
this interface can use it. A URI alone does not transfer the snapshot to Reef.

## Create metadata B

Save the complete new metadata map as `B.metadata.json`:

```json
{"zarr.json": "{\"label\":\"B\"}"}
```

```sh
python -m zarrsum.revision_refs revise-metadata A.release.json \
  --release-id PINNED_ID --revision metadata-B --metadata B.metadata.json \
  --output B.release.json > B.registration.json
```

This command reads no payload files. It keeps A's payload evidence and `payload_revision`.
It creates a distinct release ID. A remains unchanged. Bind B with its own release ID
and the same retained payload revision. Keep separate metadata files for each job.
Never replace A's metadata while a job uses A.

## Boundaries for a pilot

The existing catalog and submission examples test the earlier metadata-release format.
They do not automatically accept this new format. The reference-loader acceptance
harness makes an explicit conversion for an owned fixture. It creates separate metadata
and hard links to retained payloads. The existing four-worker Katamari loader consumes
those paths. Hard links require one filesystem and a cooperative storage owner.
Read-only permissions do not prevent that owner from changing the bytes.

Select an exact snapshot or immutable object version where the storage contract permits
it. Confirm its identity, retention period, and deletion authority. A revision string,
a checksum, and a read-only hard link do not retain data by themselves.

The published remote store still caps each payload at 32 MiB. This interface can stream
larger local files for full hashing. It does not add large-object transfer support.
A first full transfer still moves every required byte. A loader still reads the data it
consumes. Registration measurements must not be reported as transfer or training rates.

`examples/compare_revision_strategies.py` compares six modes on eight dense 128 MiB files.
It records bytes read, registration time, binding time, and metadata-update time.
It also tests a known unsampled edit and a sampled edit. Repeated reads have uncontrolled
cache state. The descriptor-only 2 PB case creates no payload files. It measures manifest
processing only. It is not a petabyte storage benchmark.

`examples/revision_loader_acceptance.py` uses the retained Reef HPA fixture.
The other catalog and submission examples also name owned Reef test resources.
They require the recorded model source and catalog-client environment. They are
acceptance harnesses. They are not general production job launchers.
