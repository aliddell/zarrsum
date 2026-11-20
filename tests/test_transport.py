"""Check the direct backend contract with real rclone and controlled child failures."""

import os
import shutil
import subprocess
import sys

import pytest

from zarrsum.transport import RcloneTransport

RCLONE = (
    os.environ.get("ZARRSUM_TEST_RCLONE")
    or shutil.which("rclone")
    or "/mnt/main0/home/aliddell/.bin/rclone"
)


def test_exact_keys_and_existing_wrong_bytes(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    key = "space +percent%/ leading trailing "
    (source / key).parent.mkdir()
    (source / key).write_bytes(b"correct")
    (destination / key).parent.mkdir()
    (destination / key).write_bytes(b"CORRUPT")
    (source / "not-selected").write_bytes(b"no")
    result = RcloneTransport(RCLONE).copy(str(source), str(destination), [key])
    assert result.ok and (destination / key).read_bytes() == b"correct"
    assert not (destination / "not-selected").exists()


def test_missing_source_with_existing_destination_fails(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (destination / "absent").write_bytes(b"old")
    with pytest.raises(ValueError, match="required files missing"):
        RcloneTransport(RCLONE).copy(str(source), str(destination), ["absent"])
    assert (destination / "absent").read_bytes() == b"old"


def test_file_does_not_expand_to_folder(tmp_path):
    source = tmp_path / "source"
    (source / "folder").mkdir(parents=True)
    (source / "folder" / "child").write_bytes(b"no")
    with pytest.raises(ValueError, match="required files missing"):
        RcloneTransport(RCLONE).copy(str(source), str(tmp_path / "dest"), ["folder"])


@pytest.mark.parametrize(
    "key", ["../escape", "/absolute", "a//b", "a\nb", "a\rb", "a\\b", "a\x00b"]
)
def test_unsupported_keys_rejected_before_process(tmp_path, key):
    backend = RcloneTransport("must-not-execute")
    with pytest.raises(ValueError):
        backend.copy(str(tmp_path), str(tmp_path / "out"), [key])
    assert backend.commands == []


def executable(tmp_path, body):
    script = tmp_path / "fake-rclone"
    script.write_text("#!" + sys.executable + "\n" + body)
    script.chmod(0o700)
    return str(script)


def test_process_failure_is_transport_failure(tmp_path):
    binary = executable(
        tmp_path, "import sys\nif 'lsf' in sys.argv: print('key')\nelse: sys.exit(7)\n"
    )
    result = RcloneTransport(binary).copy("/source", "/destination", ["key"])
    assert not result.ok and "7" in result.detail


def test_timeout_reaps_child_and_removes_temporary_files(tmp_path, monkeypatch):
    pidfile = tmp_path / "pid"
    binary = executable(
        tmp_path,
        "import os,sys,time\nfrom pathlib import Path\nif 'lsf' in sys.argv: print('key')\nelse:\n Path("
        + repr(str(pidfile))
        + ").write_text(str(os.getpid()))\n time.sleep(30)\n",
    )
    import tempfile

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    import time

    backend = RcloneTransport(binary, timeout=30)
    original_popen = subprocess.Popen

    def started(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        if "copy" in args[0]:
            deadline = time.monotonic() + 30
            while not pidfile.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            if not pidfile.exists():
                process.kill()
                process.wait()
                pytest.fail("The fake copy child did not become ready")
            backend.timeout = 0.15
        return process

    monkeypatch.setattr(subprocess, "Popen", started)
    with pytest.raises(subprocess.TimeoutExpired):
        backend.copy("/source", "/destination", ["key"])
    pid = int(pidfile.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert not list(tmp_path.glob("zarrsum-rclone-*"))


def test_signal_to_transport_owner_cleans_child(tmp_path):
    import signal
    import time

    pidfile = tmp_path / "child-pid"
    binary = executable(
        tmp_path,
        "import os,sys,time\nfrom pathlib import Path\nif 'lsf' in sys.argv: print('key')\nelse:\n Path("
        + repr(str(pidfile))
        + ").write_text(str(os.getpid()))\n time.sleep(30)\n",
    )
    code = """import signal,sys,tempfile
from zarrsum.transport import RcloneTransport
def stop(*args): raise KeyboardInterrupt()
signal.signal(signal.SIGTERM,stop)
tempfile.tempdir=sys.argv[2]
try: RcloneTransport(sys.argv[1]).copy('/source','/destination',['key'])
except KeyboardInterrupt: sys.exit(130)
"""
    owner = subprocess.Popen([sys.executable, "-c", code, binary, str(tmp_path)])
    try:
        deadline = time.monotonic() + 30
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pidfile.exists()
        owner.send_signal(signal.SIGTERM)
        assert owner.wait(timeout=30) == 130
        with pytest.raises(ProcessLookupError):
            os.kill(int(pidfile.read_text()), 0)
        assert not list(tmp_path.glob("zarrsum-rclone-*"))
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait()
