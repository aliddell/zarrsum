"""Optional Hermes file adapter inside a bounded local worker, without job submission."""

from dataclasses import asdict
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from .transport import RcloneTransport, TransferResult, file_keys


MAX_BATCH_KEYS = 256
MESSAGE_BYTES = 1 << 20


def hermes_identity():
    try:
        from hermes_core.adapters.rclone import RcloneAdapter
    except ImportError as error:
        raise ValueError(
            "Hermes backend needs the optional hermes-core package"
        ) from error
    if not callable(getattr(RcloneAdapter, "run_files", None)):
        raise ValueError("Hermes backend needs the reviewed run_files patch")
    from hermes_core.adapters import rclone

    return {
        "package_version": importlib.metadata.version("hermes-core"),
        "adapter_sha256": hashlib.sha256(
            Path(rclone.__file__).read_bytes()
        ).hexdigest(),
        "api": "run_files",
    }


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


class HermesTransport(RcloneTransport):
    max_batch_keys = MAX_BATCH_KEYS

    def __init__(self, *args, cancelled=lambda: False, **kwargs):
        self.cancelled = cancelled
        super().__init__(*args, **kwargs)
        self.hermes = hermes_identity()
        self.child_pids = []
        self.worker_pids = []

    def provenance(self):
        value = super().provenance()
        value.update(
            backend="hermes-library",
            hermes=self.hermes,
            execution="local isolated worker; no Hermes job service",
            wrapper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        )
        return value

    def copy(self, source, destination, keys):
        keys = file_keys(keys)
        if len(keys) > self.max_batch_keys:
            raise ValueError("Hermes file adapter accepts at most 256 keys")
        for root in (source, destination):
            if (
                not isinstance(root, str)
                or not root
                or root.startswith("-")
                or "://" in root
                or any(c in root for c in "\n\r\x00")
            ):
                raise ValueError(
                    "Hermes file adapter needs local or configured rclone roots"
                )
        identity = self.binary_identity()
        if self._version is None:
            self.version()
        request = dict(
            source=source,
            destination=destination,
            keys=keys,
            binary=identity["path"],
            binary_sha256=identity["sha256"],
            config=self.config,
            hermes=self.hermes,
            transfers=self.transfers,
            checkers=self.checkers,
            bandwidth=self.bandwidth,
        )
        raw = json.dumps(request).encode()
        if len(raw) > MESSAGE_BYTES:
            raise ValueError("Hermes request exceeds byte budget")
        with tempfile.TemporaryDirectory(prefix="zarrsum-hermes-") as directory:
            directory = Path(directory)
            (directory / "request.json").write_bytes(raw)
            environment = {
                name: value
                for name, value in os.environ.items()
                if not name.startswith(("AWS_", "RCLONE_", "HERMES_"))
            }
            environment["AWS_EC2_METADATA_DISABLED"] = "true"
            environment["TMPDIR"] = str(directory)
            with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
                process = subprocess.Popen(
                    [sys.executable, "-m", __name__, "--worker", str(directory)],
                    stdout=output,
                    stderr=errors,
                    env=environment,
                    start_new_session=True,
                )
                self.worker_pids.append(process.pid)
                try:
                    deadline = time.monotonic() + self.timeout
                    while process.poll() is None:
                        if self.cancelled():
                            raise InterruptedError("Managed transfer cancelled or fenced")
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise subprocess.TimeoutExpired(process.args, self.timeout)
                        try:
                            process.wait(timeout=min(0.1, remaining))
                        except subprocess.TimeoutExpired:
                            pass
                    if process.returncode:
                        stop_group(process)
                except BaseException:
                    stop_group(process)
                    raise
                finally:
                    trace = directory / "commands.jsonl"
                    if trace.exists():
                        if trace.stat().st_size > MESSAGE_BYTES:
                            raise ValueError(
                                "Hermes command evidence exceeds byte budget"
                            )
                        for line in trace.read_text().splitlines():
                            try:
                                event = json.loads(line)
                            except ValueError:
                                if process.returncode:
                                    continue
                                raise
                            self.commands.append(event["command"])
                            if event.get("pid"):
                                self.child_pids.append(event["pid"])
                response = directory / "response.json"
                if not response.exists():
                    errors.seek(0)
                    detail = errors.read(4096).decode(errors="replace")
                    return TransferResult(
                        False,
                        "failed",
                        detail or f"Hermes worker exit {process.returncode}",
                    )
                if response.stat().st_size > MESSAGE_BYTES:
                    raise ValueError("Hermes response exceeds byte budget")
                result = json.loads(response.read_bytes())
                if process.returncode:
                    result["ok"] = False
                return TransferResult(**result)


def worker(directory):
    from hermes_core.adapters import rclone as native
    from hermes_core.models import TransferSpec

    request = json.loads((directory / "request.json").read_bytes())
    if hermes_identity() != request["hermes"]:
        raise ValueError("Hermes implementation changed before worker execution")
    guard = RcloneTransport(request["binary"], expected_sha256=request["binary_sha256"])
    guard.binary_identity()
    processes = []

    def trace(command, pid=None):
        with (directory / "commands.jsonl").open("a") as stream:
            stream.write(json.dumps(dict(command=command, pid=pid)) + "\n")

    class AuditedProcesses:
        def run(self, command, *args, **kwargs):
            trace(command)
            return subprocess.run(command, *args, **kwargs)

        def Popen(self, command, *args, **kwargs):
            process = subprocess.Popen(command, *args, **kwargs)
            processes.append(process)
            trace(command, process.pid)
            return process

        def __getattr__(self, name):
            return getattr(subprocess, name)

    class Adapter(native.RcloneAdapter):
        def _base_cmd(self):
            guard.binary_identity()
            return super()._base_cmd()

        def _copy_flags(self, *args, **kwargs):
            flags = super()._copy_flags(*args, **kwargs)
            flags += [
                "--ignore-times",
                "--ignore-checksum",
                "--low-level-retries",
                "1",
                "--timeout",
                "30s",
                "--contimeout",
                "10s",
            ]
            if request["bandwidth"]:
                flags += ["--bwlimit", request["bandwidth"]]
            return flags

    native.subprocess = AuditedProcesses()
    adapter = Adapter(
        binary=request["binary"],
        config_path=request["config"],
        poll_seconds=0.05,
        stats_interval="100ms",
    )
    adapter._msf_transfers = str(request["transfers"])
    adapter._msf_checkers = str(request["checkers"])
    adapter._s3_chunk_size, adapter._s3_upload_concurrency = "5M", "2"
    spec = TransferSpec(
        source_path=request["source"], dest_path=request["destination"], overwrite=True
    )
    if (spec.source_path, spec.dest_path) != (
        request["source"],
        request["destination"],
    ):
        raise ValueError("Hermes normalization changed a pinned root")
    try:
        result = adapter.run_files(spec, request["keys"])
        response = dict(
            ok=result.ok,
            outcome=result.outcome.value,
            detail=result.detail,
            stats=asdict(result.stats) if result.stats is not None else None,
        )
        (directory / "response.json").write_text(json.dumps(response))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":

    def interrupted(*_):
        raise KeyboardInterrupt("Hermes local worker interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    if len(sys.argv) != 3 or sys.argv[1] != "--worker":
        raise SystemExit("internal worker requires a private request directory")
    worker(Path(sys.argv[2]))
