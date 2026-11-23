# Optional Hermes file transport

This is an experimental Python adapter. It calls Hermes's file-only library API in
a local isolated worker. It does not submit a Hermes job or use DynamoDB, a daemon,
the public API, or the catalog. The default commands need no Hermes installation.

The optional environment needs Python 3.11 or later and an approved `hermes-core`
build with `RcloneAdapter.run_files`. The reviewed implementation is Hermes
`fef49c5` on `agent/bruno`. It includes the retained file-only patch and Reef's
absolute cluster-path fixes.
An unpatched package fails when this backend is selected. It does not fall back
to recursive folder transfer.

```python
from zarrsum.hermes_transport import HermesTransport
from zarrsum.remotes import Boundary

transport = HermesTransport(
    binary="/approved/tools/rclone",
    config="/private/rclone.conf",
    expected_sha256=approved_rclone_sha256,
    timeout=300,
)
boundary = Boundary(
    sdk_client,
    "/private/rclone.conf",
    evidence_directory,
    prefix="retained",
    bucket="release-test",
    rclone_remote="payload",
    transport=transport,
    batch_size=64,
)
```

Use local paths or configured rclone roots at this boundary. Location resolution
must supply these roots before transfer. Raw platform URIs and public S3 URIs are
not accepted by this prototype. The wrapper refuses root normalization changes.
Each copy accepts at most 256 exact relative file keys and a 512 KiB encoded list.
The boundary refuses a larger batch before remote access. The default batch stays 64.

Hermes checks source and destination membership. The wrapper forces the copy of
selected files, including same-size private destination files. It never expands a
missing exact file into a folder. The release layer still verifies every required
BLAKE3 digest, refuses corrupt retained objects, admits cache bytes, and publishes
the final root. Transfer success alone is not release verification.

The native executable is checked on the caller and in the local worker. Receipts
identify the `hermes-library` backend, Hermes adapter source hash, wrapper source
hash, package version, and native path, hash, version, and pin status. This is local
worker provenance. It is not evidence from a distributed Hermes executor.

The deadline covers the complete worker call. Cancellation and worker failure stop
the owned process group. Calls use private temporary files. The per-batch process
startup and executable hash add overhead; this correctness prototype has not had a
throughput comparison. A managed backend should choose its job granularity separately.

No `--transport hermes` command or managed job interface is exposed. Those need the
separate service contract for exact plan binding, request reconciliation, worker
identity, access, and cancellation. Existing strong verification remains enabled.

## Managed submission experiment

[MANAGED-TRANSFERS.md](MANAGED-TRANSFERS.md) specifies a separate injected-service
experiment for durable intent, exact job reconciliation, cancellation fences, and
bound worker receipts. Its tests use a fake service and local fixture bytes. It has
no deployed Hermes client and does not enable a managed transport in `Boundary`.

## Real local control-plane acceptance candidate

The candidate `hermes_jobs` module connects the proposed contract to gated routes in
the real Hermes application. It supplies an injected HTTP client and a local executor
callback for the real runner. The test uses the real JobStore with mocked DynamoDB,
the retained exact-file adapter, native rclone, private MinIO, and existing release
admission. It does not configure or upgrade a deployed managed service.

`ExactServiceClient` uses deterministic intent lookup and owner-scoped job reads.
It can recover a lost submission response and a completely new client after restart.
`ExactExecutor` checks the actual worker and executable hashes. It observes cancellation
and the current execution fence, stops the native group, and records the real receipt.

`ManagedHermesTransport` is selected explicitly through Python `Boundary(transport=...)`.
It needs the selected release ID, its validated graph entries, native pin, service client,
and an owned record directory. It fetches payload batches into separate private paths.
It requires the bound stopped-worker receipt and checks each required BLAKE3 digest
before it returns bytes to the existing Boundary admission path. Boundary then performs
its own full check and publishes the cache release root. Transfer success alone does
not prove release identity.

The controller accepts a configured rclone source root for this experiment. The API's
deployment policy must authorize its mapping. The worker receives its private rclone
configuration from the application, not from an API request. Only local destination
paths are supported in this candidate. The default batch remains bounded to 256 keys.

If stop acknowledgement is unavailable, the transport retains the original private
staging path and writes `quarantine.json`. It does not copy uncertain output elsewhere
and then remove a path that an executor can still write. An uncertain submission or
an existing batch record can require manual reconciliation. The bridge does not yet
provide a restartable production CLI or automatic recovery for every timeout.

The receipts describe stopped native transfer groups. They do not claim that a SLURM
job or Kubernetes pod has terminated. The local acceptance uses ASGI HTTP requests and
injected proxy identity headers. It does not test a network proxy or deployed SSO.
See the matching Hermes candidate's `EXACT-FILES.md` for the API and runner contract.
