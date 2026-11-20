"""Recover submission progress without repeating an uncertain scheduler dispatch.

Callbacks must reconcile their own partial effects. A durable dispatch intent
permits one submit call. Later attempts require positive scheduler evidence or
an existing recorded job ID. An empty scheduler lookup never authorizes retry.
"""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import tempfile
import time

STAGES = ("preparation", "registration", "snapshot")
MAX_DOCUMENT = 4 << 20


class ContractMismatch(ValueError):
    pass


class ReconciliationRequired(RuntimeError):
    pass


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def sha(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(4 << 20):
            value.update(block)
    return value.hexdigest()


def read_document(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_DOCUMENT + 1)
    if len(raw) > MAX_DOCUMENT:
        raise ContractMismatch("Submission document exceeds the size bound")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ContractMismatch("Duplicate submission document field")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=unique)
    except json.JSONDecodeError as error:
        raise ContractMismatch("Invalid submission JSON document") from error


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_document(path, value):
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix=".journal-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def immutable_bytes(path, value):
    path = Path(path)
    if path.exists() or path.is_symlink():
        if path.is_symlink() or path.read_bytes() != value:
            raise ContractMismatch("Saved artifact differs: " + str(path))
        return
    descriptor, temporary = tempfile.mkstemp(prefix=".artifact-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        Path(temporary).chmod(0o444)
        os.link(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def verify_inputs(contract):
    if not re.fullmatch("[0-9a-f]{64}", contract.get("release_id", "")):
        raise ContractMismatch("An exact release ID is required")
    if contract.get("payload_policy") not in {"fresh-bytes", "trusted-retained-stat"}:
        raise ContractMismatch("An explicit payload verification policy is required")
    if not contract.get("input_files"):
        raise ContractMismatch("Source provenance must be pinned")
    for value in contract["input_files"]:
        path = Path(value["path"])
        if not path.is_absolute() or not path.is_file() or sha(path) != value["sha256"]:
            raise ContractMismatch("Input provenance changed: " + str(path))


def output_hashes(root, paths):
    values = {}
    for name in paths:
        path = Path(name)
        try:
            if path.is_symlink() or not path.resolve(strict=True).is_relative_to(root):
                raise ContractMismatch("Stage artifact escaped the submission record")
            if not stat.S_ISREG(path.stat().st_mode):
                raise ContractMismatch("Stage artifact must be a regular file")
            values[str(path.resolve())] = sha(path)
        except OSError as error:
            raise ContractMismatch("Saved stage artifact is unavailable") from error
    if not values:
        raise ContractMismatch("A completed stage must retain evidence")
    return values


class Journal:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)

    @contextmanager
    def locked(self):
        path = self.root / "controller.lock"
        if path.is_symlink():
            raise ContractMismatch("Invalid submission lock")
        with path.open("a") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ReconciliationRequired("Another controller owns this record") from error
            yield

    def event(self, kind, **fields):
        with (self.root / "events.jsonl").open("ab") as stream:
            stream.write(encoded({"event": kind, "time_ns": time.time_ns(), **fields}))
            stream.flush()
            os.fsync(stream.fileno())

    def resume(self, contract, callbacks, renderer, scheduler, hook=lambda *args: None, verifier=lambda: None):
        with self.locked():
            return self._resume(contract, callbacks, renderer, scheduler, hook, verifier)

    def _resume(self, contract, callbacks, renderer, scheduler, hook, verifier):
        verify_inputs(contract)
        if scheduler.namespace != contract.get("scheduler_namespace"):
            raise ContractMismatch("Scheduler namespace changed")
        digest = hashlib.sha256(encoded(contract)).hexdigest()
        contract_path = self.root / "contract.json"
        state_path = self.root / "state.json"
        if state_path.exists() and not contract_path.exists():
            raise ContractMismatch("The saved submission contract is missing")
        if not state_path.exists() and any((self.root/name).exists() for name in
                ["events.jsonl", "dispatch-intent.json", "job.sbatch"]):
            raise ReconciliationRequired("Submission progress is missing; do not create another dispatch")
        immutable_bytes(contract_path, encoded(contract))
        state = read_document(state_path) if state_path.exists() else {
            "format": "submission-journal/experimental-1", "contract_sha256": digest,
            "owner_uid": os.getuid(), "stages": {}, "job": None, "dispatch": None,
        }
        if (not isinstance(state, dict) or state.get("format") != "submission-journal/experimental-1"
                or not isinstance(state.get("stages"), dict)
                or "dispatch" not in state or "job" not in state
                or set(state["stages"]) != set(STAGES[:len(state["stages"])])):
            raise ContractMismatch("Invalid submission state")
        if state["contract_sha256"] != digest or state["owner_uid"] != os.getuid():
            raise ContractMismatch("Submission identity or owner changed")
        for completed in state["stages"].values():
            current = output_hashes(self.root, completed["artifacts"])
            if current != completed["artifacts"]:
                raise ContractMismatch("Saved stage provenance changed")
        verifier()
        atomic_document(state_path, state)
        self.event("resume", contract_sha256=digest)
        for stage in STAGES:
            if stage in state["stages"]:
                continue
            self.event("stage-start", stage=stage)
            outcome = callbacks[stage](self.root, state["stages"])
            if outcome.get("release_id") != contract["release_id"]:
                raise ContractMismatch("Stage selected a different release")
            artifacts = output_hashes(self.root, outcome["artifacts"])
            hook("after-action", stage)
            state["stages"][stage] = {"result": outcome, "artifacts": artifacts}
            atomic_document(state_path, state)
            self.event("stage-complete", stage=stage)
        script = self.root / "job.sbatch"
        immutable_bytes(script, renderer(self.root, state["stages"]))
        script_hash = sha(script)
        intent_path = self.root / "dispatch-intent.json"
        if state.get("dispatch") is not None and not intent_path.exists():
            raise ReconciliationRequired("The dispatch intent is missing; no resubmission is permitted")
        if intent_path.exists():
            intent = read_document(intent_path)
            if state.get("dispatch") != intent:
                raise ContractMismatch("Dispatch intent differs from the saved progress")
            for key, expected in {"contract_sha256": digest, "script_sha256": script_hash,
                                  "owner_uid": os.getuid(),
                                  "scheduler_namespace": scheduler.namespace}.items():
                if intent.get(key) != expected:
                    raise ContractMismatch("Dispatch provenance changed")
            if not re.fullmatch("dv-[0-9a-f]{32}-[0-9a-f]{12}", intent.get("token", "")):
                raise ContractMismatch("Invalid dispatch token")
            if state["job"]:
                if (state["job"].get("token") != intent["token"]
                        or state["job"].get("contract_sha256") != digest
                        or not re.fullmatch("[0-9]+", str(state["job"].get("job_id", "")))):
                    raise ContractMismatch("Recorded job token changed")
                return state["job"]
            self.event("reconcile-start", token=intent["token"])
            matches = scheduler.find(intent)
            if len(matches) != 1:
                self.event("reconcile-required", matches=len(matches))
                raise ReconciliationRequired(
                    "Dispatch outcome is uncertain: expected one matching job, found "
                    + str(len(matches)) + ". No resubmission was attempted."
                )
            job_id = str(matches[0])
            recovered = True
        else:
            if state["job"]:
                raise ContractMismatch("Recorded job has no dispatch intent")
            if hasattr(scheduler, "preflight"):
                scheduler.preflight(script)
            intent = {"token": "dv-" + secrets.token_hex(16) + "-" + digest[:12],
                      "contract_sha256": digest, "script_sha256": script_hash,
                      "owner_uid": os.getuid(), "created_epoch": time.time(),
                      "scheduler_namespace": scheduler.namespace}
            state["dispatch"] = intent
            atomic_document(state_path, state)
            immutable_bytes(intent_path, encoded(intent))
            self.event("dispatch-intent", token=intent["token"])
            hook("after-intent", None)
            self.event("submit-call-start", token=intent["token"])
            try:
                job_id = str(scheduler.submit(script, intent["token"]))
            except Exception as error:
                self.event("dispatch-uncertain", exception=type(error).__name__)
                raise ReconciliationRequired(
                    "The submit call did not establish a durable outcome. Reconcile this intent."
                ) from error
            hook("after-acceptance", job_id)
            recovered = False
        if not re.fullmatch("[0-9]+", job_id):
            raise ReconciliationRequired("Scheduler returned an unsupported job identifier")
        state["job"] = {"state": "submitted", "job_id": job_id, "token": intent["token"],
                        "contract_sha256": digest, "reconciled": recovered}
        atomic_document(state_path, state)
        self.event("job-recorded", **state["job"])
        return state["job"]


class SlurmScheduler:
    def __init__(self, runner=subprocess.run):
        self.runner = runner
        result = self._run(["scontrol", "show", "config"])
        matched = re.search(r"^ClusterName\s*=\s*(\S+)", result, re.MULTILINE)
        if not matched:
            raise ReconciliationRequired("Cannot establish the current SLURM cluster")
        self.namespace = matched[1]

    def _run(self, command):
        try:
            result = self.runner(command, capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ReconciliationRequired("Scheduler query or submission did not complete") from error
        if result.returncode:
            raise ReconciliationRequired("Scheduler command failed: " + command[0])
        return result.stdout

    def preflight(self, script):
        self._run(["sbatch", "--test-only", str(script)])

    def submit(self, script, token):
        output = self._run(["sbatch", "--parsable", "--comment=" + token,
                            "--job-name=" + token[:35], str(script)]).strip()
        if not re.fullmatch(r"[0-9]+(?:;[A-Za-z0-9_.-]+)?", output):
            raise ReconciliationRequired("Unrecognized sbatch response")
        parts = output.split(";")
        if len(parts) == 2 and parts[1] != self.namespace:
            raise ReconciliationRequired("sbatch selected another scheduler namespace")
        return parts[0]

    def find(self, intent):
        if intent["scheduler_namespace"] != self.namespace or intent["owner_uid"] != os.getuid():
            raise ContractMismatch("Cannot reconcile another scheduler or owner")
        uid = str(intent["owner_uid"])
        start = time.strftime("%Y-%m-%d", time.gmtime(intent["created_epoch"] - 300))
        outputs = [self._run(["squeue", "-h", "-u", uid, "-o", "%i|%U|%200k"]),
                   self._run(["sacct", "-n", "-P", "-X", "-u", uid, "-S", start,
                              "-o", "JobIDRaw,UID,Comment%200,State"])]
        jobs = set()
        for output in outputs:
            for line in output.splitlines():
                fields = [field.strip() for field in line.split("|")]
                if len(fields) < 3 or fields[2] != intent["token"]:
                    continue
                if fields[1] != uid or not fields[0].isdigit():
                    raise ReconciliationRequired("Matching token has uncertain owner or job identity")
                jobs.add(fields[0])
        return sorted(jobs, key=int)
