"""Copy bounded exact-key batches. Release code must validate the resulting bytes."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path, PurePosixPath
import signal
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
    ):
        if not 1 <= transfers <= 64 or not 1 <= checkers <= 64:
            raise ValueError("concurrency must be between 1 and 64")
        self.binary, self.config = str(binary), str(config or os.devnull)
        self.transfers, self.checkers = transfers, checkers
        self.timeout, self.bandwidth = timeout, bandwidth
        self.commands = []

    def _run(self, arguments):
        command = [self.binary, "--config", self.config, *arguments]
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
        return output.decode().splitlines()[0]

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
