# Publish, find, and prepare an exact reference release

This experimental workflow connects `release-reference/experimental-1` documents to
the Scientific Dataset Catalog. It uses the actual catalog request and response models.
The API adapter was tested against catalog-client 0.7.0. The acceptance example uses
an owned loopback HTTP fixture. No deployed catalog is required for that example.

The existing `zarrsum-release` commands continue to serve the separate retained-object
release format and its strong S3 recovery path. These formats are not interchangeable.

## What is shared

The publisher creates these files under the selected shared document store:

```text
release-documents/RELEASE_ID/
  release.json
  locations.json
  catalog-requests.json
  publication-complete.json
```

`release.json` contains declared membership, payload evidence, and text metadata.
Its canonical document hash is the release ID. `locations.json` supplies ordered exact
payload copies with Hermes-shaped `storage_platform` and `location_uri` fields.
It is outside the release identity. A job freezes those location hints in its resolution.

The catalog has one ordinary dataset record per supplied dataset template.
Each record uses `(canonical_id, version=RELEASE_ID, project)` as its coordinates.
Its file asset points to `release.json`. The asset has the SHA-256 of that exact file,
including its final newline. This file checksum differs from the canonical release ID.
The workflow uses the existing catalog fields. It does not add another catalog schema.

The publication-complete marker is written only after every requested catalog record
has been registered and read back. A partial publication cannot resolve successfully.
Retries can reconcile completed registration. Changed artifacts or an existing catalog
version that points elsewhere cause refusal. The marker means reference publication
completed. It does not mean a model loader validated the dataset.

## Requirements

Use the installed `zarrsum-reference` entry point, or
`python -m zarrsum.reference_workflow`. The environment needs `blake3`, the package's
`remote` extra, and the actual catalog client. The retained acceptance kit supplies a
pinned client and environment. Install the catalog client from your authorized source
for another environment. The base hashing CLI does not require that client.

Configure `CATALOG_API_TOKEN` through your normal private environment configuration.
The token is not placed in release documents or receipts. The catalog URL is explicit.
Publishing against a deployed catalog makes real catalog writes. Use the supplied
loopback fixture when you want a local demonstration.

The current reference workflow supports mounted Reef and Bruno filesystem locations.
`reef://project/path` resolves under `/mnt/main0/projects/project/path`.
`sf_hpc:///absolute/path` applies on the matching HPC execution platform.
This does not establish a route between clusters. This iteration does not transfer S3
reference payloads. Use an accessible mounted copy. URI syntax does not grant access.

## 1. Create the release document

Use `zarrsum.revision_refs register` with your chosen evidence mode. A descriptor must
declare all files. Registration does not discover undeclared files. For example:

```sh
python -m zarrsum.revision_refs register descriptor.json \
  --mode producer-digest --output A.release.json > A.registration.json
```

A release ID identifies assertions if the chosen mode contains assertions. It does not
turn a producer revision or a partial fingerprint into a full-content digest.
The registered metadata revision is in the document. Dataset version in the catalog
uses the complete release ID. Keep the published document for later reproduction.

## 2. Publish the document and catalog records

Create `publication.json`:

```json
{
  "store": {
    "storage_platform": "reef",
    "location_uri": "reef://MY_PROJECT/release-documents"
  },
  "payload_locations": [
    {
      "storage_platform": "reef",
      "location_uri": "reef://MY_PROJECT/retained/pixels-001"
    }
  ],
  "retention_assertion": "Describe the actual storage owner and agreed lifetime.",
  "catalog_records": [
    {
      "canonical_id": "my-dataset/my-store",
      "project": "MY_PROJECT",
      "name": "My image dataset, metadata A",
      "modality": "imaging",
      "dataset_type": "processed",
      "governance": {"access_scope": "internal"},
      "metadata": {}
    }
  ]
}
```

The placeholders must name your actual store, project, and retention agreement.
Supply scientific metadata under the actual catalog schema where applicable.
Version, location, and `is_latest` are generated. Do not put them in the templates.
Use one template per actual catalog dataset boundary. The acceptance uses 11 complete
image stores with one record per store. They share a complete input-release document.

```sh
zarrsum-reference publish A.release.json --publication publication.json \
  --catalog-url "$CATALOG_URL" --local-platform reef \
  --verification reference-only > A.published.json
```

The command checks one accessible declared payload copy using the selected policy.
It publishes only bounded reference documents. It does not copy payload bytes.
`reference-only` checks regular-file availability and lengths. `size-mtime` requires
recorded timestamps. `full` requires expected full digests and reads all payload bytes.

## 3. Find an exact version and freeze its resolution

```sh
zarrsum-reference list --canonical-id my-dataset/my-store --project MY_PROJECT \
  --catalog-url "$CATALOG_URL"
```

This lists up to 100 matching catalog records and reports truncation. It does not
claim those publications are all accessible or complete. Select an exact version ID.
There is no implicit latest selection.

```sh
zarrsum-reference resolve --canonical-id my-dataset/my-store --project MY_PROJECT \
  --release-id "$RELEASE_ID" --catalog-url "$CATALOG_URL" \
  --local-platform reef --output A.resolved.json > A.resolution-receipt.json
```

Resolution checks the exact catalog record, document ID, file checksum, location
binding, and publication marker. It writes a snapshot with the document and location
hints. Save its `snapshot_id` with the job recipe. Changed snapshots are refused.
A later B publication cannot change that frozen A snapshot.

## 4. Prepare this job's files

```sh
zarrsum-reference prepare A.resolved.json --snapshot-id "$SNAPSHOT_ID" \
  --destination /path/to/this-job/inputs --local-platform reef \
  --verification reference-only > this-job.preparation.json
```

Preparation needs no catalog connection or catalog token. It tries the declared
copies in order. It writes separate text metadata and hard-links payload files.
It checks the selected policy on the staged files before publishing the checkout.
Full checking is one payload read during this preparation. Reference-only checking
reads no payload bytes. The receipt records the selected copy and failed attempts.

The destination is caller-selected. `prepared/`, `A/`, and `B/` are not mandatory names.
An existing destination is refused. The destination must be separate from retained
source roots. Preparation publishes a new checkout after the stage completes.
Cooperating preparers serialize on a sibling lock file. The storage owner remains trusted.

Hard links require the same filesystem. A cross-filesystem link fails explicitly;
there is no silent full copy. Hard links do not protect bytes from their owner.
Do not modify shared payload files. Keep the exact retained revision available.
A different mount or permissions failure can select another declared copy of the same
release. All inaccessible copies cause refusal. No latest version is substituted.

Pass this checkout to the loader adapter. The included worker demonstrates the
existing Katamari CPU data path. It stops before model construction. This command
does not submit a general training job or resume model checkpoints. The older durable
submission journal requires callbacks for this new workflow before it can submit it.

## Metadata B

Create B using `revise-metadata`. Publish B with its own document and corresponding
catalog templates. It gets a different release ID and separate publication directory.
Resolve B only for jobs that select B. Existing A snapshots and checkouts stay on A.
An A job must not resolve latest again during execution.

Payload retention, source immutability, and the chosen verification strength remain
separate promises. The tools do not create snapshots or enforce a provider hold.
