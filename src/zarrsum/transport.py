"""Copy bounded exact-key batches. Release code must validate the resulting bytes."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from typing import Protocol


@dataclass
class TransferResult:
    ok: bool
    outcome: str
    detail: str = ""
    stats: dict | None = None


class Transport(Protocol):
    def copy(
        self, source: str, destination: str, keys: list[str]
    ) -> TransferResult: ...


MAX_BATCH_KEYS = 1024
MAX_KEY_LIST_BYTES = 512 << 10


class ExecutableError(ValueError):
    """The selected native executable did not pass its preflight or change fence."""


def binary_sha256(value):
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(c not in "0123456789abcdefABCDEF" for c in value)
    ):
        raise ValueError("expected a 64-character SHA-256 executable digest")
    return value.lower()


def _binary_state(value):
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
    )


def file_keys(keys):
    if not keys or len(keys) > MAX_BATCH_KEYS:
        raise ValueError("expected 1 to 1024 distinct file keys")
    for key in keys:
        if (
            not isinstance(key, str)
            or not key
            or str(PurePosixPath(key)) != key
            or key.startswith("/")
            or any(p in (".", "..") for p in key.split("/"))
            or any(c in key for c in "\\\n\r\x00")
        ):
            raise ValueError("unsupported or unsafe file key")
    if len(set(keys)) != len(keys):
        raise ValueError("expected distinct file keys")
    if sum(len(key.encode("utf-8")) + 1 for key in keys) > MAX_KEY_LIST_BYTES:
        raise ValueError("encoded key list exceeds byte budget")
    return keys


class RcloneTransport:
    def __init__(
        self,
        binary="rclone",
        config=None,
        *,
        transfers=8,
        checkers=8,
        timeout=3600,
        bandwidth=None,
        expected_sha256=None,
    ):
        if not 1 <= transfers <= 64 or not 1 <= checkers <= 64:
            raise ValueError("concurrency must be between 1 and 64")
        self.binary, self.config = str(binary), str(config or os.devnull)
        self.transfers, self.checkers = transfers, checkers
        self.timeout, self.bandwidth = timeout, bandwidth
        self.expected_sha256 = (
            binary_sha256(expected_sha256) if expected_sha256 is not None else None
        )
        self._identity = self._state = self._version = None
        self.commands = []

    def binary_identity(self):
        """Hash once per operation and refuse observed executable changes before use."""
        if self._identity is not None:
            if _binary_state(Path(self._identity["path"]).stat()) != self._state:
                raise ExecutableError("rclone executable changed after verification")
            return dict(self._identity)
        found = shutil.which(self.binary)
        if found is None:
            raise ExecutableError("rclone executable is missing or not executable")
        path = Path(found).resolve(strict=True)
        with path.open("rb") as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ExecutableError("rclone executable must be a regular file")
            digest = hashlib.sha256()
            while block := stream.read(1 << 20):
                digest.update(block)
            after = os.fstat(stream.fileno())
        if _binary_state(before) != _binary_state(after) or _binary_state(
            after
        ) != _binary_state(path.stat()):
            raise ExecutableError("rclone executable changed during verification")
        actual = digest.hexdigest()
        if self.expected_sha256 is not None and actual != self.expected_sha256:
            raise ExecutableError("rclone executable SHA-256 mismatch")
        self._state = _binary_state(after)
        self._identity = dict(
            path=str(path),
            requested=self.binary,
            sha256=actual,
            expected_sha256=self.expected_sha256,
            pin_verified=self.expected_sha256 is not None,
        )
        return dict(self._identity)

    def provenance(self):
        self.binary_identity()
        if self._version is None:
            self.version()
        return {
            "backend": "direct-rclone",
            "binary": self.binary_identity(),
            "version": self._version,
        }

    def _run(self, arguments):
        identity = self.binary_identity()
        command = [identity["path"], "--config", self.config, *arguments]
        self.commands.append(command)
        environment = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith(("AWS_", "RCLONE_"))
        }
        environment["AWS_EC2_METADATA_DISABLED"] = "true"
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            process = subprocess.Popen(
                command,
                stdout=output,
                stderr=errors,
                env=environment,
                start_new_session=True,
            )
            try:
                process.wait(timeout=self.timeout)
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=3)
                except (ProcessLookupError, subprocess.TimeoutExpired):
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                raise
            output.seek(0)
            errors.seek(0)
            data = output.read((1 << 20) + 1)
            detail = errors.read(4096).decode("utf-8", errors="replace")
            if len(data) > 1 << 20:
                raise ValueError("rclone response exceeds batch limit")
            return process.returncode, data, detail

    def version(self):
        status, output, _ = self._run(["version"])
        if status:
            raise ValueError("rclone version failed")
        lines = output.decode().splitlines()
        if not lines:
            raise ValueError("rclone version returned no output")
        self._version = lines[0]
        return self._version

    def copy(self, source, destination, keys):
        keys = file_keys(keys)
        for root in (source, destination):
            if (
                not isinstance(root, str)
                or not root
                or root.startswith("-")
                or any(c in root for c in "\n\r\x00")
            ):
                raise ValueError("invalid transport root")
        with tempfile.TemporaryDirectory(prefix="zarrsum-rclone-") as directory:
            files = Path(directory) / "keys.txt"
            files.write_text("\n".join(keys) + "\n", encoding="utf-8")
            common = [
                "--files-from-raw",
                str(files),
                "--retries",
                "1",
                "--low-level-retries",
                "1",
            ]

            def present(root):
                status, output, detail = self._run(
                    ["lsf", root, *common, "-R", "--files-only"]
                )
                if status:
                    raise ValueError(
                        "rclone source/destination probe failed: " + detail
                    )
                found = set(output.decode("utf-8").splitlines())
                missing = set(keys) - found
                if missing:
                    raise ValueError(
                        "required files missing: " + repr(sorted(missing)[:4])
                    )

            present(source)
            log = Path(directory) / "stats.jsonl"
            arguments = [
                "copy",
                source,
                destination,
                *common,
                "--no-traverse",
                "--ignore-times",
                "--ignore-checksum",
                "--transfers",
                str(self.transfers),
                "--checkers",
                str(self.checkers),
                "--s3-chunk-size",
                "5M",
                "--s3-upload-concurrency",
                "2",
                "--use-json-log",
                "--log-file",
                str(log),
                "--stats",
                "100ms",
                "--stats-log-level",
                "NOTICE",
                "--timeout",
                "30s",
                "--contimeout",
                "10s",
            ]
            if self.bandwidth:
                arguments += ["--bwlimit", self.bandwidth]
            started = time.monotonic()
            status, _, detail = self._run(arguments)
            stats = None
            if log.exists():
                with log.open() as stream:
                    for line in stream:
                        record = json.loads(line)
                        if "stats" in record:
                            stats = record["stats"]
                        if record.get("level") in ("error", "critical"):
                            detail = str(record.get("msg", "rclone error"))[:1000]
            if status:
                return TransferResult(
                    False, "failed", detail or f"rclone exit {status}", stats
                )
            present(destination)
            return TransferResult(
                True,
                "transferred",
                f"transport only; elapsed {time.monotonic() - started:.6f}s",
                stats,
            )
