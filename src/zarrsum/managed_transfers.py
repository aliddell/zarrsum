"""Exercise a proposed managed exact-file contract without a deployed service.

The service must support exact intent lookup and return bound worker evidence.
This controller does not implement the current Hermes HTTP API or publish roots.
"""

import hashlib
import os
from pathlib import Path
import re
import secrets

from .submissions import (
    ContractMismatch, Journal, ReconciliationRequired, atomic_document, encoded,
    immutable_bytes, read_document,
)
from .transport import file_keys


def fingerprint(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def exact_request(value):
    """Bind a small local fixture plan before any service operation."""
    fields = {"release_id", "plan_sha256", "source", "staging_parent", "keys",
              "native_sha256", "worker_sha256"}
    if not isinstance(value, dict) or set(value) != fields:
        raise ContractMismatch("Invalid managed file request fields")
    for name in ("release_id", "plan_sha256", "native_sha256", "worker_sha256"):
        if not isinstance(value[name], str) or not re.fullmatch("[0-9a-f]{64}", value[name]):
            raise ContractMismatch("Invalid request digest: " + name)
    keys = file_keys(value["keys"])
    if not keys or len(keys) > 256 or list(keys) != value["keys"]:
        raise ContractMismatch("Expected 1 to 256 exact file keys")
    source_name = value["source"]
    configured_source = (isinstance(source_name, str)
                         and re.fullmatch(r"[A-Za-z0-9_-]+:[^\n\r\x00]+", source_name)
                         and "://" not in source_name)
    source, parent = (Path(value[name]) for name in ("source", "staging_parent"))
    for path in ((parent,) if configured_source else (source, parent)):
        if not path.is_absolute() or str(path.resolve()) != str(path):
            raise ContractMismatch("The local contract requires canonical absolute roots")
    if not configured_source and (source.is_relative_to(parent) or parent.is_relative_to(source)):
        raise ContractMismatch("Source and private staging roots must be separate")
    if len(encoded(value)) > 1 << 20:
        raise ContractMismatch("Managed request exceeds the document bound")
    return value


class ManagedTransfer:
    """Keep one durable dispatch intent and reject uncertain or late results."""

    def __init__(self, root, service):
        self.journal = Journal(root)
        self.service = service

    def _save(self, state):
        atomic_document(self.journal.root / "managed-state.json", state)

    def _load(self):
        root = self.journal.root
        state = read_document(root / "managed-state.json")
        intent = read_document(root / "managed-intent.json")
        if (state.get("intent_sha256") != fingerprint(intent)
                or intent.get("namespace") != self.service.namespace
                or intent.get("owner_uid") != os.getuid()):
            raise ContractMismatch("Managed intent identity changed")
        return state, intent

    def _bound_job(self, job, intent):
        if not isinstance(job, dict):
            raise ContractMismatch("Missing managed job identity")
        for name, expected in {"namespace": intent["namespace"], "token": intent["token"],
                               "request_sha256": fingerprint(intent)}.items():
            if job.get(name) != expected:
                raise ContractMismatch("Managed job binding changed: " + name)
        if (not isinstance(job.get("job_id"), str)
                or not re.fullmatch("[A-Za-z0-9._:-]{1,128}", job["job_id"])):
            raise ContractMismatch("Invalid opaque job identifier")
        return job["job_id"]

    def submit(self, request, hook=lambda event: None):
        request = exact_request(request)
        root = self.journal.root
        with self.journal.locked():
            path = root / "managed-state.json"
            if not path.exists():
                if (root / "managed-intent.json").exists():
                    raise ReconciliationRequired("Saved progress is missing; do not resubmit")
                token = "dv-" + secrets.token_hex(16)
                intent = {"format": "managed-file-intent/experimental-1",
                          "namespace": self.service.namespace, "owner_uid": os.getuid(),
                          "token": token, "request": request,
                          "destination": str(Path(request["staging_parent"]) / token)}
                immutable_bytes(root / "managed-intent.json", encoded(intent))
                state = {"intent_sha256": fingerprint(intent), "job_id": None,
                         "cancel_requested": False, "phase": "intent"}
                self._save(state)
                hook("after-intent")
                try:
                    job = self.service.submit(intent)
                except Exception as error:
                    raise ReconciliationRequired("Submission uncertain; reconcile the saved intent") from error
                hook("after-acceptance")
            else:
                state, intent = self._load()
                if intent["request"] != request:
                    raise ContractMismatch("Managed request changed")
                if state["job_id"]:
                    return state["job_id"]
                matches = self.service.find(intent["token"])
                if len(matches) != 1:
                    raise ReconciliationRequired("Expected one exact job; no resubmission attempted")
                job = matches[0]
            state["job_id"] = self._bound_job(job, intent)
            state["phase"] = "submitted"
            self._save(state)
            return state["job_id"]

    def cancel(self):
        with self.journal.locked():
            state, intent = self._load()
            state["cancel_requested"] = True
            state["phase"] = "cancel-requested"
            self._save(state)
            if state["job_id"] is None:
                raise ReconciliationRequired("Reconcile the job before cancelling its executor")
            job = self.service.get(state["job_id"])
            if self._bound_job(job, intent) != state["job_id"]:
                raise ContractMismatch("Cancellation selected another job")
            self.service.cancel(state["job_id"])

    def observe(self, verify):
        """Return checked private output only after bound stopped-worker evidence.

        The caller supplies full content verification. A success row alone is not
        admission evidence. A saved cancellation always prevents output acceptance.
        """
        with self.journal.locked():
            state, intent = self._load()
            if state["job_id"] is None:
                raise ReconciliationRequired("Reconcile the job before observing its result")
            job = self.service.get(state["job_id"])
            if self._bound_job(job, intent) != state["job_id"]:
                raise ContractMismatch("Managed result selected another job")
            receipt = job.get("receipt")
            if not isinstance(receipt, dict) or receipt.get("executor_stopped") is not True:
                raise ReconciliationRequired("Executor termination is unproved; keep output private")
            expected = {"job_id": state["job_id"], "token": intent["token"],
                        "request_sha256": fingerprint(intent),
                        "destination": intent["destination"],
                        "native_sha256": intent["request"]["native_sha256"],
                        "worker_sha256": intent["request"]["worker_sha256"],
                        "outcome": "SUCCEEDED"}
            if any(receipt.get(key) != value for key, value in expected.items()):
                raise ContractMismatch("Worker receipt does not bind the selected request")
            if not receipt.get("execution_id"):
                raise ContractMismatch("Worker execution identity is missing")
            if job.get("execution_id") is not None and receipt["execution_id"] != job["execution_id"]:
                raise ContractMismatch("Worker receipt belongs to another execution")
            if state["cancel_requested"] or job.get("status") != "SUCCEEDED":
                state["phase"] = "stopped-without-acceptance"
                self._save(state)
                raise ReconciliationRequired("Cancelled or unsuccessful output cannot be accepted")
            result = verify(Path(intent["destination"]), intent["request"])
            if result is not True:
                raise ContractMismatch("Full content verification did not succeed")
            state["phase"] = "verified-private-output"
            state["receipt_sha256"] = fingerprint(receipt)
            self._save(state)
            return result
