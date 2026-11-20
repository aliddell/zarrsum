"""Compare exact-file transport contracts using real Hermes, rclone, and local S3."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
from types import SimpleNamespace
import subprocess
import sys
import time

import pytest

pytest.importorskip("hermes_core")

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import psutil

from test_ozx import local_s3 as local_s3
from zarrsum.hermes_transport import HermesTransport
from zarrsum.releases import Store, digest
from zarrsum.remotes import Boundary, object_key
from zarrsum.transport import RcloneTransport


BACKENDS = ["direct", "hermes"]


def make(backend, config=None, **options):
    binary = Path(shutil.which("rclone")).resolve()
    cls = HermesTransport if backend == "hermes" else RcloneTransport
    return cls(
        binary,
        config,
        expected_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        **options,
    )


def evidence(name, backend, value):
    root = os.environ.get("CONTRACT_EVIDENCE")
    if root:
        path = Path(root)
        path.mkdir(parents=True, exist_ok=True)
        (path / f"{name}-{backend}.json").write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n"
        )


def absent(api, key):
    with pytest.raises(ClientError) as failure:
        api.get_object(Bucket="contract-test", Key=key)
    assert failure.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def fixture(tmp_path, local_s3, backend, batch_size=64):
    endpoint, profile = local_s3
    api = boto3.Session(profile_name="ozx").client(
        "s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"})
    )
    api.create_bucket(Bucket="contract-test")
    source = tmp_path / "source"
    source.mkdir()
    content = {
        "data.bin": b"correct payload\n" * 512,
        "nested/space +%.json": b'{"label":"A"}',
    }
    for key, data in content.items():
        target = source / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    store = Store(tmp_path / "publisher")
    rid = store.publish(
        source,
        {"version": 2, "partitions": 4, "files": list(content), "grids": []},
        source_id="hermes-contract",
        assume_immutable=True,
    )["release_id"]
    adapter = make(backend, profile)
    boundary = Boundary(
        api,
        profile,
        tmp_path / "evidence",
        prefix="retained",
        bucket="contract-test",
        rclone_remote="payload",
        transport=adapter,
        batch_size=batch_size,
    )
    return api, source, store, rid, content, adapter, boundary


@pytest.mark.parametrize("backend", BACKENDS)
def test_exact_keys_repair_same_size_private_destination(tmp_path, backend):
    source = tmp_path / "source"
    source.mkdir()
    dest = tmp_path / "dest"
    dest.mkdir()
    (source / "space +%").write_bytes(b"correct")
    (source / "extra").write_bytes(b"excluded")
    (dest / "space +%").write_bytes(b"CORRUPT")
    adapter = make(backend)
    result = adapter.copy(str(source), str(dest), ["space +%"])
    assert result.ok
    assert (dest / "space +%").read_bytes() == b"correct"
    assert not (dest / "extra").exists()
    provenance = adapter.provenance()
    assert provenance["binary"]["pin_verified"]
    if backend == "hermes":
        assert provenance["backend"] == "hermes-library"
    evidence(
        "exact-files",
        backend,
        dict(passed=True, provenance=provenance, commands=adapter.commands),
    )


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("kind", ["folder", "stale-target"])
def test_missing_source_never_expands_or_accepts_stale_target(tmp_path, backend, kind):
    source = tmp_path / "source"
    source.mkdir()
    dest = tmp_path / "dest"
    if kind == "folder":
        (source / "absent").mkdir()
        (source / "absent/child").write_bytes(b"not selected")
    else:
        dest.mkdir()
        (dest / "absent").write_bytes(b"old")
    adapter = make(backend)
    try:
        result = adapter.copy(str(source), str(dest), ["absent"])
    except ValueError:
        pass
    else:
        assert not result.ok
    if kind == "folder":
        assert not dest.exists()
    else:
        assert (dest / "absent").read_bytes() == b"old"
    assert not any("copy" in command for command in adapter.commands)
    evidence(kind, backend, dict(passed=True, commands=adapter.commands))


@pytest.mark.parametrize("backend", BACKENDS)
def test_root_last_retry_and_offline_checkout(tmp_path, local_s3, backend, monkeypatch):
    api, source, store, rid, content, adapter, boundary = fixture(
        tmp_path, local_s3, backend, batch_size=1
    )
    with pytest.raises(InterruptedError):
        boundary.push(store.path, rid, interrupt_after=1)
    absent(api, "retained/releases/" + rid)
    plan = tmp_path / "evidence/push.plan.json"
    original = plan.read_bytes()
    pin = json.loads((tmp_path / "evidence/push.submission.json").read_text())["blake3"]
    plan.write_bytes(original + b" ")
    command_count = len(adapter.commands)
    with pytest.raises(ValueError):
        boundary.push(store.path, rid, resume_plan=plan, plan_digest=pin)
    assert len(adapter.commands) == command_count
    absent(api, "retained/releases/" + rid)
    plan.write_bytes(original)
    pushed = boundary.push(store.path, rid, resume_plan=plan, plan_digest=pin)
    assert plan.read_bytes() == original and pushed["result"] == "verified"
    assert pushed["transport"]["binary"]["pin_verified"]
    shutil.rmtree(source)
    shutil.rmtree(store.path)
    fetched = boundary.fetch(rid, tmp_path / "cache")

    def forbidden(*args, **kwargs):
        raise AssertionError("offline checkout accessed transport")

    monkeypatch.setattr(api, "get_object", forbidden)
    monkeypatch.setattr(adapter, "copy", forbidden)
    monkeypatch.setattr(adapter, "provenance", forbidden)
    checkout = boundary.checkout(tmp_path / "cache", rid, tmp_path / "checkout")
    for key, data in content.items():
        assert (tmp_path / "checkout" / key).read_bytes() == data
    evidence(
        "roundtrip",
        backend,
        dict(
            passed=True,
            release_id=rid,
            files=len(content),
            bytes=sum(map(len, content.values())),
            push=pushed,
            fetch=fetched,
            checkout=checkout,
            altered_plan_refused=True,
            root_absent_after_interruption=True,
            plan_bytes_preserved=True,
            offline_checkout=True,
            commands=adapter.commands,
        ),
    )


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("fault", ["missing-with-descendant", "same-size-corruption"])
def test_bad_remote_bytes_are_not_admitted(tmp_path, local_s3, backend, fault):
    api, _, store, rid, content, adapter, boundary = fixture(
        tmp_path, local_s3, backend
    )
    boundary.push(store.path, rid)
    data = content["data.bin"]
    key = "retained/" + object_key(digest(data))
    if fault == "missing-with-descendant":
        api.delete_object(Bucket="contract-test", Key=key)
        api.put_object(Bucket="contract-test", Key=key + "/child", Body=data)
    else:
        api.put_object(Bucket="contract-test", Key=key, Body=b"X" * len(data))
    with pytest.raises(ValueError):
        boundary.fetch(rid, tmp_path / "cache")
    assert not (tmp_path / "cache/releases" / rid).exists()
    assert not list(tmp_path.glob(".incoming-*"))
    evidence(
        fault,
        backend,
        dict(
            passed=True,
            release_root_admitted=False,
            incoming_removed=True,
            commands=adapter.commands,
        ),
    )


@pytest.mark.parametrize("backend", BACKENDS)
def test_corrupt_existing_target_prevents_root_publication(tmp_path, local_s3, backend):
    api, _, store, rid, content, _, boundary = fixture(tmp_path, local_s3, backend)
    data = content["data.bin"]
    key = "retained/" + object_key(digest(data))
    api.put_object(Bucket="contract-test", Key=key, Body=b"X" * len(data))
    with pytest.raises(ValueError, match="corrupt remote object"):
        boundary.push(store.path, rid)
    absent(api, "retained/releases/" + rid)
    evidence("corrupt-target", backend, dict(passed=True, root_published=False))


def test_backend_batch_limit_precedes_sdk_access(tmp_path):
    adapter = make("hermes")
    client = SimpleNamespace(
        meta=SimpleNamespace(endpoint_url="http://127.0.0.1:13900")
    )
    with pytest.raises(ValueError, match="backend limit"):
        Boundary(
            client, "unused", tmp_path / "evidence", transport=adapter, batch_size=1024
        )
    assert adapter.commands == []


def test_worker_deadline_removes_processes_and_private_files(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"x" * (8 << 20))
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    adapter = make("hermes", bandwidth="64k", timeout=2)
    with pytest.raises(subprocess.TimeoutExpired):
        adapter.copy(str(source), str(tmp_path / "dest"), ["data"])
    for pid in adapter.child_pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    assert not list(tmp_path.glob("zarrsum-hermes-*"))


@pytest.mark.parametrize("victim", ["owner", "worker"])
def test_active_interruption_stops_native_children(tmp_path, victim):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"x" * (8 << 20))
    script = """import signal,sys
from zarrsum.hermes_transport import HermesTransport
def stop(*args): raise KeyboardInterrupt()
signal.signal(signal.SIGTERM,stop)
try:
 result=HermesTransport(timeout=60,bandwidth="64k").copy(sys.argv[1],sys.argv[2],["data"])
 sys.exit(0 if result.ok else 2)
except KeyboardInterrupt: sys.exit(130)
"""
    owner = subprocess.Popen(
        [sys.executable, "-c", script, str(source), str(tmp_path / "dest")],
        env={**os.environ, "TMPDIR": str(tmp_path)},
    )
    native_pid = None
    worker_pid = None
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and owner.poll() is None:
            for child in psutil.Process(owner.pid).children(recursive=True):
                try:
                    args = child.cmdline()
                    if "--worker" in args:
                        worker_pid = child.pid
                    if args and Path(args[0]).name == "rclone" and "copy" in args:
                        native_pid = child.pid
                except psutil.NoSuchProcess:
                    pass
            if native_pid and worker_pid:
                break
            time.sleep(0.05)
        assert native_pid is not None
        assert worker_pid is not None
        if victim == "owner":
            owner.terminate()
        else:
            os.kill(worker_pid, signal.SIGKILL)
        code = owner.wait(timeout=10)
        assert code == (130 if victim == "owner" else 2)
        deadline = time.monotonic() + 3
        state = None
        while time.monotonic() < deadline:
            try:
                state = psutil.Process(native_pid).status()
            except psutil.NoSuchProcess:
                state = None
                break
            if state == psutil.STATUS_ZOMBIE:
                break
            time.sleep(0.02)
        assert state in (None, psutil.STATUS_ZOMBIE)
        if victim == "owner":
            assert state is None
        assert not list(tmp_path.glob("zarrsum-hermes-*"))
        evidence(
            "active-" + victim + "-loss",
            "hermes",
            dict(
                passed=True,
                owner_exit=code,
                native_pid=native_pid,
                native_reaped=state is None,
                native_running=False,
                private_files_removed=True,
            ),
        )
    finally:
        if owner.poll() is None:
            owner.terminate()
            owner.wait(timeout=10)
