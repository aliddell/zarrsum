"""Inject managed control-plane failures without AWS or a deployed Hermes API."""

import copy
import json
import os
from pathlib import Path
import shutil

from blake3 import blake3
import pytest

from zarrsum.managed_transfers import ManagedTransfer, fingerprint
from zarrsum.submissions import ContractMismatch, ReconciliationRequired, read_document


class FakeService:
    namespace = "owned-fake-hermes"

    def __init__(self):
        self.jobs = {}
        self.submissions = 0
        self.lose_submit = False
        self.lose_cancel = False

    def submit(self, intent):
        self.submissions += 1
        job_id = "job-20bfb679-3395-49b7-86e4-f32b3f3038ae"
        self.jobs[job_id] = {
            "job_id": job_id, "token": intent["token"], "namespace": self.namespace,
            "request_sha256": fingerprint(intent), "status": "REQUESTED",
            "intent": copy.deepcopy(intent),
        }
        if self.lose_submit:
            raise TimeoutError("accepted; response lost")
        return copy.deepcopy(self.jobs[job_id])

    def find(self, token):
        return [copy.deepcopy(j) for j in self.jobs.values() if j["token"] == token]

    def get(self, job_id):
        return copy.deepcopy(self.jobs[job_id])

    def cancel(self, job_id):
        self.jobs[job_id]["status"] = "CANCELLED"
        if self.lose_cancel:
            raise TimeoutError("cancel accepted; response lost")

    def finish(self, job_id):
        job = self.jobs[job_id]
        intent = job["intent"]
        request = intent["request"]
        destination = Path(intent["destination"])
        destination.mkdir(parents=True)
        for key in request["keys"]:
            target = destination / key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(Path(request["source"]) / key, target)
        job["status"] = "SUCCEEDED"
        job["receipt"] = {name: job[name] for name in ("job_id", "token", "request_sha256")}
        job["receipt"].update(
            destination=str(destination), executor_stopped=True, execution_id="fake-executor-1",
            native_sha256=request["native_sha256"], worker_sha256=request["worker_sha256"],
            outcome="SUCCEEDED",
        )


@pytest.fixture
def case(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    files = {"data": b"pixels", "nested/space +%.json": b'{"label":"A"}'}
    for key, raw in files.items():
        path = source / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    (source / "unrequested").write_bytes(b"must stay absent")
    manifest = {key: {"bytes": len(raw), "blake3": blake3(raw).hexdigest()}
                for key, raw in files.items()}
    plan = {"release_id": "a" * 64, "plan_sha256": fingerprint(manifest),
            "source": str(source), "staging_parent": str(tmp_path / "private"),
            "keys": list(files), "native_sha256": "b" * 64, "worker_sha256": "c" * 64}
    service = FakeService()
    root = tmp_path / "journal"
    verified = []

    def verify(destination, request):
        assert request == plan
        assert {p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file()} == set(files)
        for key, expected in manifest.items():
            path = destination / key
            if path.stat().st_size != expected["bytes"] or blake3(path.read_bytes()).hexdigest() != expected["blake3"]:
                raise ValueError("full content mismatch")
        verified.append(str(destination))
        return True

    return root, service, plan, verify, verified


def test_lost_response_reconciles_one_opaque_job_and_exact_bytes(case):
    root, service, plan, verify, verified = case
    service.lose_submit = True
    with pytest.raises(ReconciliationRequired, match="uncertain"):
        ManagedTransfer(root, service).submit(plan)
    job_id = ManagedTransfer(root, service).submit(plan)
    assert service.submissions == 1 and not job_id.isdigit()
    assert ManagedTransfer(root, service).submit(plan) == job_id
    service.finish(job_id)
    result = ManagedTransfer(root, service).observe(verify)
    assert result is True and len(verified) == 1
    assert read_document(root / "managed-state.json")["phase"] == "verified-private-output"
    output = os.environ.get("MANAGED_CONTRACT_EVIDENCE")
    if output:
        target = Path(output)
        target.mkdir(parents=True, exist_ok=True)
        (target / "lost-response.json").write_text(json.dumps({
            "passed": True, "submit_calls": service.submissions, "job": service.get(job_id),
            "state": read_document(root / "managed-state.json"), "files": len(plan["keys"]),
            "scope": "fake service; local copy; full fixture BLAKE3; no release publication",
        }, indent=2) + "\n")


@pytest.mark.parametrize("count", [0, 2])
def test_empty_or_ambiguous_lookup_never_resubmits(case, count):
    root, service, plan, _, _ = case
    service.lose_submit = True
    with pytest.raises(ReconciliationRequired):
        ManagedTransfer(root, service).submit(plan)
    job = next(iter(service.jobs.values()))
    service.find = lambda _: [job] * count
    with pytest.raises(ReconciliationRequired, match="no resubmission"):
        ManagedTransfer(root, service).submit(plan)
    assert service.submissions == 1


@pytest.mark.parametrize("event", ["after-intent", "after-acceptance"])
def test_crash_recovery_never_repeats_submit(case, event):
    root, service, plan, _, _ = case
    def crash(actual):
        if actual == event:
            raise SystemExit("controller lost")
    with pytest.raises(SystemExit):
        ManagedTransfer(root, service).submit(plan, crash)
    if event == "after-acceptance":
        assert ManagedTransfer(root, service).submit(plan)
        assert service.submissions == 1
    else:
        with pytest.raises(ReconciliationRequired):
            ManagedTransfer(root, service).submit(plan)
        assert service.submissions == 0


@pytest.mark.parametrize("field", ["plan_sha256", "release_id", "native_sha256", "worker_sha256", "keys", "source", "staging_parent"])
def test_changed_request_refused_without_another_submission(case, field):
    root, service, plan, _, _ = case
    ManagedTransfer(root, service).submit(plan)
    changed = copy.deepcopy(plan)
    changed[field] = list(reversed(plan[field])) if field == "keys" else ("d" * 64 if field.endswith("sha256") or field == "release_id" else plan[field] + "-other")
    with pytest.raises(ContractMismatch):
        ManagedTransfer(root, service).submit(changed)
    assert service.submissions == 1


@pytest.mark.parametrize("field", ["request_sha256", "token", "native_sha256", "worker_sha256", "destination", "job_id", "outcome", "execution_id"])
def test_wrong_worker_receipt_never_reaches_verification(case, field):
    root, service, plan, verify, verified = case
    control = ManagedTransfer(root, service)
    job_id = control.submit(plan)
    service.finish(job_id)
    service.jobs[job_id]["receipt"][field] = ""
    with pytest.raises(ContractMismatch):
        control.observe(verify)
    assert not verified


def test_success_row_alone_is_insufficient(case):
    root, service, plan, verify, verified = case
    control = ManagedTransfer(root, service)
    job_id = control.submit(plan)
    service.jobs[job_id]["status"] = "SUCCEEDED"
    with pytest.raises(ReconciliationRequired, match="termination"):
        control.observe(verify)
    assert not verified


@pytest.mark.parametrize("lost_response", [False, True])
def test_cancellation_fence_survives_restart_and_late_success(case, lost_response):
    root, service, plan, verify, verified = case
    control = ManagedTransfer(root, service)
    job_id = control.submit(plan)
    service.lose_cancel = lost_response
    if lost_response:
        with pytest.raises(TimeoutError):
            control.cancel()
    else:
        control.cancel()
    with pytest.raises(ReconciliationRequired, match="termination"):
        ManagedTransfer(root, service).observe(verify)
    service.finish(job_id)
    with pytest.raises(ReconciliationRequired, match="cannot be accepted"):
        ManagedTransfer(root, service).observe(verify)
    assert not verified and service.submissions == 1


@pytest.mark.parametrize("fault", ["missing", "same-size"])
def test_full_bytes_gate_refuses_bad_staged_output(case, fault):
    root, service, plan, verify, verified = case
    control = ManagedTransfer(root, service)
    job_id = control.submit(plan)
    service.finish(job_id)
    target = Path(service.jobs[job_id]["intent"]["destination"]) / "data"
    if fault == "missing":
        target.unlink()
    else:
        target.write_bytes(b"broken")
    with pytest.raises((AssertionError, ValueError)):
        control.observe(verify)
    assert not verified
    assert read_document(root / "managed-state.json")["phase"] == "submitted"


def test_lookup_failure_cannot_trigger_resubmission(case):
    root, service, plan, _, _ = case
    service.lose_submit = True
    with pytest.raises(ReconciliationRequired):
        ManagedTransfer(root, service).submit(plan)
    def unavailable(_):
        raise TimeoutError("lookup unavailable")
    service.find = unavailable
    with pytest.raises(TimeoutError):
        ManagedTransfer(root, service).submit(plan)
    assert service.submissions == 1


def test_cancellation_before_reconciliation_still_fences_late_success(case):
    root, service, plan, verify, verified = case
    service.lose_submit = True
    control = ManagedTransfer(root, service)
    with pytest.raises(ReconciliationRequired):
        control.submit(plan)
    with pytest.raises(ReconciliationRequired, match="Reconcile"):
        control.cancel()
    job_id = ManagedTransfer(root, service).submit(plan)
    service.finish(job_id)
    with pytest.raises(ReconciliationRequired, match="cannot be accepted"):
        ManagedTransfer(root, service).observe(verify)
    assert not verified and service.submissions == 1


def test_foreign_lookup_and_cancellation_are_refused(case):
    root, service, plan, _, _ = case
    service.lose_submit = True
    with pytest.raises(ReconciliationRequired):
        ManagedTransfer(root, service).submit(plan)
    job = next(iter(service.jobs.values()))
    job["request_sha256"] = "d" * 64
    with pytest.raises(ContractMismatch):
        ManagedTransfer(root, service).submit(plan)
    assert service.submissions == 1
    job["request_sha256"] = fingerprint(read_document(root / "managed-intent.json"))
    control = ManagedTransfer(root, service)
    job_id = control.submit(plan)
    job["token"] = "foreign-token"
    with pytest.raises(ContractMismatch):
        control.cancel()
    assert service.jobs[job_id]["status"] == "REQUESTED"


def test_missing_progress_and_competing_controller_refuse_submit(case):
    root, service, plan, _, _ = case
    control = ManagedTransfer(root, service)
    with control.journal.locked():
        with pytest.raises(ReconciliationRequired, match="Another controller"):
            ManagedTransfer(root, service).submit(plan)
    assert service.submissions == 0
    control.submit(plan)
    (root / "managed-state.json").unlink()
    with pytest.raises(ReconciliationRequired, match="progress is missing"):
        ManagedTransfer(root, service).submit(plan)
    assert service.submissions == 1


def test_false_verifier_result_is_not_acceptance(case):
    root, service, plan, _, _ = case
    control = ManagedTransfer(root, service)
    job_id = control.submit(plan)
    service.finish(job_id)
    with pytest.raises(ContractMismatch, match="verification did not succeed"):
        control.observe(lambda *args: False)
    assert read_document(root / "managed-state.json")["phase"] == "submitted"
