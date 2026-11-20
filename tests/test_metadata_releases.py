import json
import os
import signal
import subprocess
import sys
import time

import pytest

from zarrsum.metadata_releases import Repository, canonical


def fixture(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "zarr.json").write_bytes(canonical({
        "zarr_format": 3, "node_type": "group", "attributes": {"revision": "A"}}))
    (source / "chunk").write_bytes(b"unchanged pixels")
    repo = Repository(tmp_path / "repo")
    release, _ = repo.publish("fixture", "A", source, ["zarr.json", "chunk"], ["zarr.json"])
    return repo, release


def ready(repo, release, destination):
    repo.stage(release, "reef", destination)
    repo.promote(release, "reef", dict(result="passed", release_id=release,
                                      input_root=str(destination)))


def test_metadata_update_keeps_ready_a_and_shares_only_payload(tmp_path):
    repo, a = fixture(tmp_path)
    a_path = tmp_path / "a"
    ready(repo, a, a_path)
    bound = repo.bind("fixture", a, "reef")
    repo.recommend("fixture", a, "reef")
    b, cost = repo.update(a, "B", {"zarr.json": canonical({
        "zarr_format": 3, "node_type": "group", "attributes": {"revision": "B"}})})
    assert cost["payload_bytes_read"] == 0 and cost["publisher_trust"]
    with pytest.raises(ValueError, match="not ready"):
        repo.bind("fixture", b, "reef")
    b_path = tmp_path / "b"
    repo.stage(b, "reef", b_path)
    with pytest.raises(ValueError, match="not ready"):
        repo.bind("fixture", b, "reef")
    assert repo.recommendation("fixture") == a
    repo.promote(b, "reef", dict(result="passed", release_id=b, input_root=str(b_path)))
    repo.recommend("fixture", b, "reef")
    assert repo.recommendation("fixture") == b
    assert bound["input_root"] == str(a_path)
    assert json.loads((a_path / "zarr.json").read_bytes())["attributes"]["revision"] == "A"
    assert json.loads((b_path / "zarr.json").read_bytes())["attributes"]["revision"] == "B"
    assert (a_path / "chunk").stat().st_ino == (b_path / "chunk").stat().st_ino
    assert (a_path / "zarr.json").stat().st_ino != (b_path / "zarr.json").stat().st_ino


@pytest.mark.parametrize("replacement", ["chunk", "../escape", "absent"])
def test_refuse_payload_or_membership_updates(tmp_path, replacement):
    repo, a = fixture(tmp_path)
    with pytest.raises(ValueError):
        repo.update(a, "B", {replacement: b"new"})


def test_refuse_array_structure_change(tmp_path):
    repo, a = fixture(tmp_path)
    with pytest.raises(ValueError, match="new ingest"):
        repo.update(a, "B", {"zarr.json": canonical({
            "zarr_format": 3, "node_type": "array", "attributes": {}})})


def test_refuse_reused_revision(tmp_path):
    repo, a = fixture(tmp_path)
    with pytest.raises(ValueError, match="cannot be reused"):
        repo.update(a, "A", {"zarr.json": canonical({
            "zarr_format": 3, "node_type": "group", "attributes": {"revision": "B"}})})


def test_refuse_mutated_payload_and_keep_ready_binding_closed(tmp_path):
    repo, a = fixture(tmp_path)
    path = tmp_path / "a"
    ready(repo, a, path)
    payload = path / "chunk"
    previous = payload.stat()
    payload.chmod(0o644)
    payload.write_bytes(b"damaged contents")
    payload.chmod(0o444)
    os.utime(payload, ns=(previous.st_atime_ns, previous.st_mtime_ns + 1_000_000_000))
    with pytest.raises(ValueError):
        repo.bind("fixture", a, "reef")


def test_trusted_reuse_cannot_detect_same_size_same_mtime_change(tmp_path):
    repo, a = fixture(tmp_path)
    path = tmp_path / "a"
    ready(repo, a, path)
    payload = path / "chunk"
    previous = payload.stat()
    size = previous.st_size
    payload.chmod(0o644)
    payload.write_bytes(b"x" * size)
    payload.chmod(0o444)
    os.utime(payload, ns=(previous.st_atime_ns, previous.st_mtime_ns))
    assert repo.bind("fixture", a, "reef")["release_id"] == a
    assert payload.read_bytes() == b"x" * size


def test_refuse_changed_checkout_metadata(tmp_path):
    repo, a = fixture(tmp_path)
    path = tmp_path / "a"
    ready(repo, a, path)
    (path / "zarr.json").chmod(0o644)
    (path / "zarr.json").write_bytes(b"{}")
    with pytest.raises(ValueError, match="metadata mismatch"):
        repo.bind("fixture", a, "reef")


def test_refuse_wrong_loader_receipt(tmp_path):
    repo, a = fixture(tmp_path)
    repo.stage(a, "reef", tmp_path / "a")
    with pytest.raises(ValueError, match="matching loader"):
        repo.promote(a, "reef", dict(result="passed", release_id="0" * 64,
                                     input_root=str(tmp_path / "a")))


def test_refuse_symlink_and_preserve_existing_destination(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "link").symlink_to(__file__)
    repo = Repository(tmp_path / "repo")
    with pytest.raises(ValueError):
        repo.publish("fixture", "A", source, ["link"], [])
    repo, a = fixture_second(tmp_path)
    destination = tmp_path / "destination"
    destination.mkdir()
    (destination / "sentinel").write_text("keep")
    with pytest.raises(FileExistsError):
        repo.stage(a, "reef", destination)
    assert (destination / "sentinel").read_text() == "keep"


def fixture_second(tmp_path):
    nested = tmp_path / "second"
    nested.mkdir()
    return fixture(nested)


def test_update_and_stage_do_not_open_payloads(tmp_path):
    repo, a = fixture(tmp_path)
    payloads = {str(repo.object_path(entry["blake3"]))
                for entry in repo.manifest(a)["entries"] if entry["kind"] == "payload"}
    active = [True]

    def audit(event, args):
        if active[0] and event == "open" and str(args[0]) in payloads:
            raise AssertionError("The metadata update read a retained payload")

    sys.addaudithook(audit)
    try:
        b, _ = repo.update(a, "B", {"zarr.json": canonical({
            "zarr_format": 3, "node_type": "group", "attributes": {"revision": "B"}})})
        repo.stage(b, "reef", tmp_path / "b")
    finally:
        active[0] = False


def test_file_count_bound_and_membership_duplicates(tmp_path):
    repo = Repository(tmp_path / "repo")
    for paths in (["same", "same"], [str(n) for n in range(10_001)]):
        with pytest.raises(ValueError, match="membership"):
            repo.publish("fixture", "A", tmp_path, paths, [])


def test_actual_kill_during_staging_never_makes_release_ready(tmp_path):
    repo, a = fixture(tmp_path)
    destination = tmp_path / "interrupted"
    marker = tmp_path / "member-staged"
    code = """
import sys, time
from pathlib import Path
from zarrsum.metadata_releases import Repository
def pause(path):
    Path(sys.argv[4]).write_text(path)
    time.sleep(60)
Repository(sys.argv[1]).stage(sys.argv[2], 'reef', sys.argv[3], after_member=pause)
"""
    child = subprocess.Popen([sys.executable, "-c", code, str(repo.root), a,
                              str(destination), str(marker)])
    try:
        until = time.monotonic() + 10
        while not marker.exists() and time.monotonic() < until:
            if child.poll() is not None:
                raise AssertionError("Stage process exited before its marker")
            time.sleep(0.02)
        assert marker.exists()
        child.send_signal(signal.SIGKILL)
        assert child.wait(timeout=10) == -signal.SIGKILL
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
    assert not destination.exists()
    with pytest.raises(ValueError, match="not ready"):
        repo.bind("fixture", a, "reef")
    assert list(tmp_path.glob(".staging-*"))
    ready(repo, a, destination)
    assert repo.bind("fixture", a, "reef")["release_id"] == a


def test_explicit_shared_pool_survives_host_local_device_remap(tmp_path,monkeypatch):
    from zarrsum import metadata_releases as module
    root=tmp_path/'shared'
    identity={'fstype':'nfs','export_path':'/fixture-volume','root':str(root),'pool_id':'a'*32}
    monkeypatch.setattr(module,'nfs_pool_identity',lambda path:{**identity,'pool_id':'b'*32})
    source=tmp_path/'input';source.mkdir()
    (source/'zarr.json').write_text('{}');(source/'chunk').write_bytes(b'pixels')
    repo=Repository(root,shared_filesystem=identity)
    release,_=repo.publish('fixture','A',source,['zarr.json','chunk'],['zarr.json'])
    destination=tmp_path/'A'
    ready(repo,release,destination)
    remapped=Repository(root,shared_filesystem=identity)
    original=module.stamp
    remapped._local_device+=100
    def another_host(path):
        value=original(path);value[0]+=100;return value
    monkeypatch.setattr(module,'stamp',another_host)
    assert remapped.bind('fixture',release,'reef')['release_id']==release
    with pytest.raises(ValueError,match='explicitly'):
        Repository(root)
    changed={**identity,'export_path':'/another-volume'}
    with pytest.raises(ValueError,match='identity differs'):
        Repository(root,shared_filesystem=changed)


def test_legacy_pool_is_not_silently_reinterpreted(tmp_path,monkeypatch):
    from zarrsum import metadata_releases as module
    repo,release=fixture(tmp_path)
    identity={'fstype':'nfs','export_path':'/fixture-volume','root':str(repo.root),'pool_id':'a'*32}
    monkeypatch.setattr(module,'nfs_pool_identity',lambda path:identity)
    with pytest.raises(ValueError,match='Cannot reinterpret legacy'):
        Repository(repo.root,shared_filesystem=identity)
    assert not (repo.root/'shared-filesystem.json').exists()
