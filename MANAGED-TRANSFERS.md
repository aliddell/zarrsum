# Proposed managed file-transfer contract

`zarrsum.managed_transfers` is a local control-plane experiment. It uses an injected
service interface. It has no HTTP client and does not implement the deployed Hermes
API. It does not submit a SLURM job, use DynamoDB, or publish a release root.
The existing direct and Hermes library transports do not change.

The experiment reuses the submission journal's lock, bounded JSON reader, atomic
state writes, and immutable intent file. It accepts opaque job IDs. One saved intent
permits one submission call. After a lost response or controller restart, exact lookup
must return one job with the same namespace, token, and complete request digest.
An empty lookup, multiple matches, or lookup failure never permits another submission.
A crash after intent creation and before submission can require manual reconciliation.
This conservative behavior does not promise automatic recovery in every case.

The service interface has these methods:

| Method | Required behavior |
|---|---|
| `submit(intent)` | Return a job bound to `fingerprint(intent)`, its token, and namespace. |
| `find(token)` | Return exact job candidates. Absence is not permission to resubmit. |
| `get(job_id)` | Return the same bound job, current status, and worker receipt. |
| `cancel(job_id)` | Request cancellation. A response does not prove executor termination. |

Each service exposes a stable `namespace`. The immutable intent contains the release
ID, caller-supplied plan SHA-256, ordered exact keys, source root, private staging
destination, owner UID, and required native and worker source SHA-256 values.
The caller must obtain the plan digest from its frozen release plan. This experiment
does not reinterpret or independently validate that release graph.

This bounded fixture supports canonical absolute local roots. The private destination
is a new token directory below the selected staging parent. It refuses overlapping
source and staging roots. It accepts 1 to 256 exact relative file keys. It uses the
existing key and encoded-list validation. This does not establish cross-site URI
translation, mounted-volume access, or remote destination policy.

Cancellation writes a durable local fence before any service call. The controller
checks the job binding before it sends a cancellation request. A cancellation timeout
preserves the fence. A later success cannot make that output eligible for verification.
The controller does not infer executor termination from a `CANCELLED` row.

Observation requires a receipt with the same job, token, complete intent digest,
destination, native SHA-256, and worker source SHA-256. It also requires a nonempty
execution ID, successful outcome, and explicit stopped-executor evidence. The supplied
full-content verifier must return `True`. A success row or a membership/size check is
insufficient. The test verifier checks exact membership, length, and every fixture
BLAKE3 digest. It rejects missing files and same-length corruption.

Successful observation records `verified-private-output`. It does not admit objects
into the release cache or publish a root. A real managed backend must connect this
result to the existing release admission and publication gates. Receipt authenticity,
executor-stop evidence, runner access, queue/execution deadlines, late remote writes,
and output quarantine still need an agreed service contract.

Run `python -m pytest -q tests/test_managed_transfers.py` for the fake-service checks.
They inject response loss, controller loss, empty and ambiguous lookups, lookup failure,
changed plans and routes, incorrect worker receipts, competing controllers, missing
progress, cancellation loss, late success, missing output, and same-length corruption.
No test contacts a deployed service. These tests do not demonstrate a Hermes service
upgrade, authenticated worker provenance, or distributed stop acknowledgement.
