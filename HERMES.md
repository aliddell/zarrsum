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
