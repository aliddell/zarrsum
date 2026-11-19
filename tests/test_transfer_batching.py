"""Check bounded larger native batches without changing plan or byte semantics."""

from types import SimpleNamespace

import pytest

from zarrsum.remote_cli import main
from zarrsum.remotes import Boundary
from zarrsum.transport import RcloneTransport, TransferResult, file_keys


def test_key_count_and_encoded_bytes_have_independent_limits():
    assert len(file_keys([str(i) for i in range(1024)])) == 1024
    with pytest.raises(ValueError, match="1024"):
        file_keys([str(i) for i in range(1025)])
    with pytest.raises(ValueError, match="byte budget"):
        file_keys([str(i) + "x" * 1024 for i in range(600)])
    with pytest.raises(ValueError, match="distinct"):
        file_keys(["same", "same"])


def test_larger_batch_copies_only_exact_keys_with_real_rclone(tmp_path):
    import shutil

    binary = shutil.which("rclone")
    if not binary:
        pytest.skip("rclone is unavailable")
    source, target = tmp_path / "source", tmp_path / "target"
    source.mkdir()
    target.mkdir()
    names = [f"key {i} +%" for i in range(257)]
    for key in names:
        (source / key).write_bytes(b"correct")
        (target / key).write_bytes(b"CORRUPT")
    (source / "not-selected").write_bytes(b"excluded")
    assert RcloneTransport(binary).copy(str(source), str(target), names).ok
    assert not (target / "not-selected").exists()
    assert all((target / key).read_bytes() == b"correct" for key in names)
    (source / names[-1]).unlink()
    with pytest.raises(ValueError, match="required files missing"):
        RcloneTransport(binary).copy(str(source), str(target), names)


def test_coordinator_batches_without_rewriting_pinned_plan(tmp_path):
    class Transport:
        def __init__(self):
            self.batches = []

        def copy(self, source, destination, keys):
            self.batches.append(list(keys))
            return TransferResult(True, "transferred")

    events = SimpleNamespace(register=lambda *args: None)
    client = SimpleNamespace(
        meta=SimpleNamespace(endpoint_url="http://127.0.0.1:13900", events=events)
    )
    adapter = Transport()
    b = Boundary(
        client,
        tmp_path / "unused",
        tmp_path / "evidence",
        transport=adapter,
        batch_size=1024,
    )
    entries = [[f"object/{i:05d}", 1, "a" * 64] for i in range(2050)]
    path, pin = b.plan("b" * 64, "/source", "/destination", entries, "test")
    before = path.read_bytes()
    b.execute(path, pin)
    assert list(map(len, adapter.batches)) == [1024, 1024, 2]
    assert path.read_bytes() == before
    adapter.batches.clear()
    b.batch_size = 64
    b.execute(path, pin)
    assert max(map(len, adapter.batches)) == 64
    assert path.read_bytes() == before


@pytest.mark.parametrize("size", [0, 1025, -1])
def test_cli_rejects_invalid_batch_before_configuration(size, tmp_path):
    with pytest.raises(SystemExit) as error:
        main(
            [
                "push",
                str(tmp_path),
                "a" * 64,
                "--evidence",
                str(tmp_path / "evidence"),
                "--remote",
                "unused",
                "--remotes",
                str(tmp_path / "missing"),
                "--transfer-batch-size",
                str(size),
            ]
        )
    assert error.value.code == 2
