"""Check computed membership, retained recovery, and publication failure behavior."""
import json
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from zarrsum.releases import Store, descriptor, keys, main
import zarrsum.releases as releases


@pytest.fixture
def sample(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    spec = {"version": 1, "files": ["zarr.json", "stats.json"], "grids": [
        {"prefix": "cells/c", "grid_shape": [12, 3]},
        {"prefix": "sparse/c", "grid_shape": [3, 3], "present": [[0, 0], [2, 2]]},
    ]}
    normalized = descriptor(spec)
    for index, key in enumerate(keys(normalized)):
        target = source / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(f"content-{index}".encode())
    return source, spec, Store(tmp_path / "retained")


def publish(store, source, spec, **kwargs):
    options = dict(source_id="fixed-fixture-A", assume_immutable=True)
    options.update(kwargs)
    return store.publish(source, spec, **options)


def test_restore_after_source_change_and_delete(sample, tmp_path):
    source, spec, store = sample
    expected = {key: (source / key).read_bytes() for key in keys(descriptor(spec))}
    result = publish(store, source, spec)
    for key in expected:
        (source / key).write_bytes(b"replacement")
        (source / key).unlink()
    target = tmp_path / "restored"
    assert store.restore(result["release_id"], target)["ok"]
    assert {key: (target / key).read_bytes() for key in expected} == expected
    assert store.verify(result["release_id"], target)["files"] == len(expected)
    assert store.verify(result["release_id"])["ok"]


def test_worker_order_and_descriptor_order_do_not_change_identity(sample, tmp_path):
    source, spec, store = sample
    first = publish(store, source, spec, workers=1)
    spec["files"].reverse()
    spec["grids"].reverse()
    spec["grids"][0]["present"].reverse()
    second = publish(Store(tmp_path / "other"), source, spec, workers=4)
    assert first["release_id"] == second["release_id"]


def test_no_source_directory_listing(sample):
    source, spec, store = sample
    scan, listing = os.scandir, os.listdir

    def guarded(original):
        def call(path=".", *args, **kwargs):
            resolved = Path(os.readlink(f"/proc/self/fd/{path}") if isinstance(path, int) else path).resolve()
            if resolved == source or resolved.is_relative_to(source):
                raise AssertionError("source directory listing")
            return original(path, *args, **kwargs)
        return call

    with patch("os.scandir", guarded(scan)), patch("os.listdir", guarded(listing)):
        result = publish(store, source, spec)
        assert store.verify(result["release_id"], source)["ok"]


def test_sparse_membership_and_unrelated_files(sample):
    source, spec, store = sample
    (source / "sparse/c/0/0").read_bytes()
    assert not (source / "sparse/c/1/1").exists()
    result = publish(store, source, spec)
    (source / "unrelated").write_bytes(b"outside membership")
    assert store.verify(result["release_id"], source)["ok"]
    assert result["files"] == 40


def test_large_grid_streams_without_materializing_coordinate_axes():
    spec = descriptor({"version": 1, "files": [],
                       "grids": [{"prefix": "a", "grid_shape": [1_000_000_000]}]})
    generated = keys(spec)
    assert next(generated) == "a/0"
    assert next(generated) == "a/1"


@pytest.mark.parametrize("key", ["/absolute", "../escape", "a/../b", "a//b", "./a", "a/", "a\\b", "", "a\x00b"])
def test_unsafe_membership_rejected(key):
    with pytest.raises(ValueError):
        descriptor({"version": 1, "files": [key], "grids": []})


@pytest.mark.parametrize("files,grids", [
    (["a", "a"], []),
    (["a", "a-b", "a/b"], []),
    (["a/0"], [{"prefix": "a", "grid_shape": [1]}]),
    ([], [{"prefix": "a", "grid_shape": [2], "present": [[1], [1]]}]),
    ([], [{"prefix": "a", "grid_shape": [2], "present": [[2]]}]),
    ([], [{"prefix": "a", "grid_shape": [True]}]),
    ([], []),
])
def test_invalid_descriptor_rejected(files, grids):
    with pytest.raises(ValueError):
        descriptor({"version": 1, "files": files, "grids": grids})


@pytest.mark.parametrize("separator,files", [("/", ["a/.zarray", "a/stats.json"]),
                                            (".", ["a/.zarray", "a/stats.json"])])
def test_metadata_and_grid_can_share_array_directory(separator, files):
    spec = descriptor({"version": 1, "files": files,
                       "grids": [{"prefix": "a", "grid_shape": [2, 3], "separator": separator}]})
    assert len(list(keys(spec))) == 8


def test_missing_required_member_publishes_no_release(sample):
    source, spec, store = sample
    (source / "cells/c/2/1").unlink()
    with pytest.raises(FileNotFoundError):
        publish(store, source, spec)
    assert list((store.path / "releases").iterdir()) == []


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_source_symlink_rejected(sample, tmp_path, kind):
    source, spec, store = sample
    if kind == "file":
        (source / "stats.json").unlink()
        (source / "stats.json").symlink_to(source / "zarr.json")
    else:
        (source / "cells/c/0").rename(tmp_path / "elsewhere")
        (source / "cells/c/0").symlink_to(tmp_path / "elsewhere", target_is_directory=True)
    with pytest.raises(OSError):
        publish(store, source, spec)
    assert not list((store.path / "releases").iterdir())


def test_resume_keeps_completed_partitions(sample):
    source, spec, store = sample
    def stop(count):
        if count == 2:
            raise RuntimeError("interrupted publisher")
    with pytest.raises(RuntimeError, match="interrupted"):
        publish(store, source, spec, workers=1, progress=stop)
    assert not list((store.path / "releases").iterdir())
    result = publish(store, source, spec, workers=2)
    assert result["reused_partitions"] == 2
    assert 0 < result["source_files_read"] < result["files"]
    assert store.verify(result["release_id"])["ok"]


def test_resume_different_source_id_rehashes(sample):
    source, spec, store = sample
    first = publish(store, source, spec)
    second = publish(store, source, spec, source_id="fixed-fixture-B")
    assert second["source_files_read"] == second["files"]
    assert first["release_id"] == second["release_id"]


def test_metadata_delta_reads_only_changed_file(sample):
    source, spec, store = sample
    first = publish(store, source, spec)
    original_metadata = (source / "zarr.json").read_bytes()
    (source / "zarr.json").write_bytes(b"metadata revision B")
    second = publish(store, source, spec, source_id="fixed-fixture-B", parent=first["release_id"],
                     changed=["zarr.json"], assume_complete_changes=True)
    assert second["source_files_read"] == 1
    assert second["release_id"] != first["release_id"]
    assert second["reused_partitions"] == first["partitions"] - 1
    first_record = next(r for r in store.records(first["release_id"]) if r[0] == "zarr.json")
    assert store.object_path(first_record[2]).read_bytes() == original_metadata
    assert store.verify(first["release_id"])["ok"]
    assert store.verify(second["release_id"])["ok"]


def test_data_delta_matches_fresh_publication(sample, tmp_path):
    source, spec, store = sample
    first = publish(store, source, spec)
    key = "cells/c/1/2"
    (source / key).write_bytes(b"new cell")
    second = publish(store, source, spec, source_id="fixed-fixture-B", parent=first["release_id"],
                     changed=[key], assume_complete_changes=True)
    fresh = publish(Store(tmp_path / "fresh"), source, spec, workers=1)
    assert second["release_id"] == fresh["release_id"]
    assert second["source_files_read"] == 1


def test_delta_requires_complete_change_assertion(sample):
    source, spec, store = sample
    first = publish(store, source, spec)
    with pytest.raises(ValueError, match="complete"):
        publish(store, source, spec, parent=first["release_id"], changed=[])
    with pytest.raises(ValueError, match="outside"):
        publish(store, source, spec, parent=first["release_id"], changed=["unknown"],
                assume_complete_changes=True)


def test_asserted_incomplete_delta_is_not_a_fresh_check(sample):
    source, spec, store = sample
    first = publish(store, source, spec)
    (source / "stats.json").write_bytes(b"unreported change")
    second = publish(store, source, spec, source_id="fixed-fixture-B", parent=first["release_id"],
                     changed=[], assume_complete_changes=True)
    assert second["release_id"] == first["release_id"]
    with pytest.raises(ValueError, match="destination bytes"):
        store.verify(second["release_id"], source)


def test_corruption_with_preserved_size_and_mtime(sample, tmp_path):
    source, spec, store = sample
    result = publish(store, source, spec)
    record = next(store.records(result["release_id"]))
    obj = store.object_path(record[2])
    before = obj.stat()
    obj.write_bytes(b"x" * before.st_size)
    os.utime(obj, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(ValueError, match="retained bytes"):
        store.verify(result["release_id"])
    with pytest.raises(ValueError):
        store.restore(result["release_id"], tmp_path / "bad-restore")
    assert not (tmp_path / "bad-restore").exists()
    with pytest.raises(ValueError):
        publish(store, source, spec)


def test_partition_corruption_detected(sample):
    source, spec, store = sample
    result = publish(store, source, spec)
    root = json.loads((store.path / "releases" / result["release_id"]).read_bytes())
    (store.path / "manifests" / root["partitions"][0][1]).write_bytes(b"[]")
    with pytest.raises(ValueError, match="corrupt manifests"):
        store.verify(result["release_id"])


def test_restore_refuses_existing_destination(sample, tmp_path):
    source, spec, store = sample
    result = publish(store, source, spec)
    destination = tmp_path / "existing"
    destination.mkdir()
    (destination / "mine").write_bytes(b"preserve")
    with pytest.raises(FileExistsError):
        store.restore(result["release_id"], destination)
    assert (destination / "mine").read_bytes() == b"preserve"


def test_source_and_store_cannot_overlap(sample):
    source, spec, _store = sample
    with pytest.raises(ValueError, match="separate"):
        publish(Store(source / "retained"), source, spec)


def test_source_assertion_required(sample):
    source, spec, store = sample
    with pytest.raises(ValueError, match="immutable"):
        store.publish(source, spec, source_id="mutable")


def test_concurrent_publisher_rejected(sample):
    source, spec, store = sample
    with open(store.path / "publisher.lock", "a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            publish(store, source, spec)


def test_mid_read_mutation_rejected(sample, monkeypatch):
    source, spec, store = sample
    original = releases.open_member

    @contextmanager
    def changing(root, key):
        with original(root, key) as stream:
            class Reader:
                changed = False

                def fileno(self):
                    return stream.fileno()

                def read(self, size):
                    block = stream.read(size)
                    if block and not self.changed:
                        self.changed = True
                        (Path(root) / key).write_bytes(b"changed during read")
                    return block
            yield Reader()

    monkeypatch.setattr(releases, "open_member", changing)
    with pytest.raises(ValueError, match="source changed"):
        publish(store, source, spec, workers=1)
    assert not list((store.path / "releases").iterdir())


def test_read_permission_error_rejects_publication(sample, monkeypatch):
    source, spec, store = sample
    def denied(*args, **kwargs):
        raise PermissionError("injected member access error")
    monkeypatch.setattr(releases, "open_member", denied)
    with pytest.raises(PermissionError):
        publish(store, source, spec)
    assert not list((store.path / "releases").iterdir())


def test_cli_round_trip(sample, tmp_path, capsys):
    source, spec, store = sample
    description = tmp_path / "descriptor.json"
    description.write_text(json.dumps(spec))
    assert main(["publish", str(store.path), str(source), str(description), "--source-id", "fixture",
                 "--assume-immutable-source"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert main(["restore", str(store.path), result["release_id"], str(tmp_path / "cli-restore")]) == 0
    assert main(["verify", str(store.path), result["release_id"]]) == 0
