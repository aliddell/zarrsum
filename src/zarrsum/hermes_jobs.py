"""Local acceptance bridge for the experimental real Hermes exact-file routes.

The application injects HTTP credentials, service namespace, executor, and roots.
No deployment or default transport selection changes.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import tempfile
import time

from .hermes_transport import HermesTransport, hermes_identity
from .managed_transfers import ManagedTransfer, fingerprint
from .submissions import encoded
from .transport import TransferResult, file_keys


def worker_identity():
    from . import hermes_transport

    return fingerprint(
        {
            "executor": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "wrapper": hashlib.sha256(Path(hermes_transport.__file__).read_bytes()).hexdigest(),
            "hermes": hermes_identity(),
        }
    )


def groups_stopped(groups):
    deadline = time.monotonic() + 3
    pending = set(groups)
    while pending:
        for group in list(pending):
            try:
                os.killpg(group, 0)
            except ProcessLookupError:
                pending.remove(group)
        if not pending:
            break
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)
    return True


class ExactServiceClient:
    def __init__(self, client, namespace):
        self.client, self.namespace = client, namespace
        self.tokens = {}

    def _result(self, response):
        response.raise_for_status()
        value = response.json()
        jobs = value if isinstance(value, list) else [value]
        for job in jobs:
            self.tokens[job["job_id"]] = job["token"]
        return value

    def submit(self, intent):
        return self._result(self.client.post("/api/v1/exact-files", json=intent))

    def find(self, token):
        return self._result(self.client.get("/api/v1/exact-files/" + token))

    def get(self, job_id):
        result = self._result(self.client.get("/api/v1/exact-jobs/" + job_id))
        if result["job_id"] != job_id:
            raise ValueError("Exact lookup changed job identity")
        return result

    def cancel(self, job_id):
        if job_id not in self.tokens:
            self.get(job_id)
        return self._result(
            self.client.post("/api/v1/exact-files/" + self.tokens[job_id] + "/cancel")
        )


class ExactExecutor:
    """Run the retained Hermes adapter and bind its real evidence to one attempt."""

    def __init__(self, binary, config, *, timeout=120, bandwidth=None):
        self.binary, self.config = binary, config
        self.timeout, self.bandwidth = timeout, bandwidth
        self.results = []

    def __call__(self, store, job, execution_id):
        from hermes_core.models import AttemptResult, JobStatus, now_iso

        intent = json.loads(job.exact_intent_json)
        request = intent["request"]
        transport = None
        native = None
        provenance = None
        result = TransferResult(False, "FAILED", "Executor did not start")

        def fenced():
            current = store.get_job(job.job_id)
            return (
                current is None
                or current.status is not JobStatus.RUNNING
                or current.execution_id != execution_id
                or current.exact_intent_json != job.exact_intent_json
            )

        try:
            if (
                request["worker_sha256"] != worker_identity()
                or request["source"] != job.source_path
                or intent["destination"] != job.dest_path
            ):
                raise ValueError("Worker or pinned roots changed")
            transport = HermesTransport(
                self.binary,
                self.config,
                timeout=self.timeout,
                bandwidth=self.bandwidth,
                expected_sha256=request["native_sha256"],
                cancelled=fenced,
            )
            native = transport.binary_identity()
            provenance = transport.provenance()
            if fenced():
                raise InterruptedError("Execution fenced before transfer")
            result = transport.copy(job.source_path, job.dest_path, request["keys"])
        except Exception as error:
            result = TransferResult(False, "FAILED", type(error).__name__ + ": " + str(error))
        cancelled = fenced()
        stopped = groups_stopped(transport.worker_pids) if transport else True
        ok = result.ok and not cancelled and stopped
        outcome = "SUCCEEDED" if ok else ("CANCELLED" if cancelled else "FAILED")
        receipt = {
            "job_id": job.job_id,
            "execution_id": execution_id,
            "token": intent["token"],
            "request_sha256": fingerprint(intent),
            "destination": job.dest_path,
            "worker_sha256": worker_identity(),
            "native_sha256": native["sha256"] if native else None,
            "outcome": outcome,
            "executor_stopped": stopped,
            "stop_scope": "isolated native worker and copy; runner writes receipt afterward",
            "native_pids": transport.child_pids if transport else [],
            "worker_groups": transport.worker_pids if transport else [],
            "commands": transport.commands if transport else [],
            "provenance": provenance,
            "detail": result.detail[:2000],
        }
        raw = encoded(receipt).decode()
        if len(raw.encode()) > 128 << 10:
            raise ValueError("Worker receipt exceeds budget")
        store.record_result(
            AttemptResult(
                job_id=job.job_id,
                execution_id=execution_id,
                attempt=job.attempts,
                ok=ok,
                outcome=outcome,
                adapter="rclone-exact",
                started_at=job.started_at,
                finished_at=now_iso(),
                runner_host=socket.gethostname(),
                exact_receipt_json=raw,
                error_tail=result.detail[:2000] or None,
            )
        )
        final = store.complete_job(
            job.job_id,
            JobStatus.SUCCEEDED if ok else JobStatus.FAILED,
            expected_execution_id=execution_id,
        )
        self.results.append(receipt)
        return 0 if final is not None and ok else 1


class ManagedHermesTransport:
    """Fetch one bounded release batch through the real API and runner fixture."""

    max_batch_keys = 256

    def __init__(
        self,
        service,
        record_root,
        release_id,
        expected,
        native_sha256,
        *,
        worker_sha256=None,
        timeout=120,
    ):
        self.service, self.record_root = service, Path(record_root)
        self.release_id, self.expected = release_id, expected
        self.native_sha256 = native_sha256
        self.worker_sha256 = worker_sha256 or worker_identity()
        self.timeout = timeout
        self.commands, self.jobs = [], []
        self.counter = 0

    def version(self):
        return "hermes-exact-api/experimental-1"

    def provenance(self):
        return {
            "backend": "hermes-exact-api",
            "namespace": self.service.namespace,
            "jobs": self.jobs,
            "native_sha256": self.native_sha256,
            "worker_sha256": self.worker_sha256,
        }

    def copy(self, source, destination, keys):
        from .remotes import local_get, validate

        keys = file_keys(keys)
        if len(keys) > self.max_batch_keys or any(key not in self.expected for key in keys):
            raise ValueError("Batch differs from the pinned release graph")
        self.counter += 1
        root = self.record_root / str(self.counter)
        root.mkdir(parents=True)
        entries = [[key, *self.expected[key]] for key in keys]
        stage = tempfile.mkdtemp(prefix="managed-private-", dir=root)
        request = {
            "source": source,
            "staging_parent": stage,
            "keys": keys,
            "release_id": self.release_id,
            "plan_sha256": fingerprint(entries),
            "native_sha256": self.native_sha256,
            "worker_sha256": self.worker_sha256,
        }
        controller = ManagedTransfer(root / "journal", self.service)
        job_id = None
        stopped = False
        try:
            job_id = controller.submit(request)
            self.jobs.append(job_id)
            deadline = time.monotonic() + self.timeout
            while True:
                job = self.service.get(job_id)
                if job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Managed job deadline; output remains private")
                time.sleep(0.05)

            def verify(path, bound):
                if bound != request:
                    raise ValueError("Worker request changed before verification")
                for key in keys:
                    validate(local_get(path, key), self.expected[key])
                return True

            controller.observe(verify)
            receipt = self.service.get(job_id)["receipt"]
            stopped = receipt["executor_stopped"] is True
            self.commands.extend(receipt["commands"])
            incoming = Path(receipt["destination"])
            for key in keys:
                target = Path(destination) / key
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(incoming / key, target)
        except BaseException:
            try:
                controller.cancel()
                until = time.monotonic() + min(self.timeout, 10)
                while job_id is not None and time.monotonic() < until:
                    receipt = self.service.get(job_id).get("receipt", {})
                    if receipt.get("executor_stopped") is True:
                        stopped = True
                        break
                    time.sleep(0.05)
            except Exception:
                pass
            raise
        finally:
            if stopped:
                shutil.rmtree(stage)
            else:
                (root / "quarantine.json").write_bytes(
                    encoded(
                        {
                            "path": stage,
                            "job_id": job_id,
                            "reason": "Executor stop is not established; retain original private path",
                        }
                    )
                )
        return TransferResult(True, "copied")
