"""Check the scale changes: recorded partitions, registry families, bounded validation, deltas."""
import importlib.util
import json
import subprocess
import time
from pathlib import Path

import pytest

import zarrsum.releases as releases
from zarrsum.releases import Store, descriptor, keys, main

ROOT = Path(__file__).resolve().parents[1]


def registry(ids):
    return "".join(i + "\n" for i in sorted(ids)).encode()


@pytest.fixture
def family(tmp_path):
    """Three cell groups with metadata, a sidecar, and a five-shard array each."""
    store = Store(tmp_path / "retained")
    ids = ["A/1/1002_372", "A/1/1002_373", "B/2/7_1"]
    value, count = store.put_registry(registry(ids))
    spec = {"version": 2, "partitions": 8, "files": ["zarr.json", "A/zarr.json", "A/1/zarr.json",
                                                      "B/zarr.json", "B/2/zarr.json"],
            "grids": [], "families": [{"registry": value, "count": count,
                                       "files": ["zarr.json", "normalization_stats.json", "0/zarr.json"],
                                       "grids": [{"prefix": "0/c", "grid_shape": [5, 1, 1]}]}]}
    source = tmp_path / "source"
    for index, key in enumerate(keys(descriptor(spec, store.registries(spec)), store.registries(spec))):
        target = source / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(f"content-{index}".encode())
    return source, spec, store, ids


def publish(store, source, spec, **kwargs):
    options = dict(source_id="fixed-fixture-A", assume_immutable=True)
    options.update(kwargs)
    return store.publish(source, spec, **options)


def original_module():
    code = subprocess.run(["git", "-C", str(ROOT), "show", "99bdb79:src/zarrsum/releases.py"],
                          capture_output=True, text=True)
    if code.returncode:
        pytest.skip("base commit 99bdb79 is not available")
    spec = importlib.util.spec_from_loader("releases_99bdb79", loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(code.stdout, module.__dict__)
    return module


def test_version_1_identity_matches_base_commit(tmp_path):
    base = original_module()
    source = tmp_path / "source"
    spec = {"version": 1, "files": ["zarr.json", "a/zarr.json"],
            "grids": [{"prefix": "a/c", "grid_shape": [4, 3]},
                      {"prefix": "b", "grid_shape": [3], "separator": ".", "present": [[2], [0]]}]}
    for index, key in enumerate(keys(descriptor(spec))):
        (source / key).parent.mkdir(parents=True, exist_ok=True)
        (source / key).write_bytes(bytes([index]) * index)
    old = base.Store(tmp_path / "old").publish(source, spec, source_id="s", assume_immutable=True)
    new = Store(tmp_path / "new").publish(source, spec, source_id="s", assume_immutable=True)
    assert old["release_id"] == new["release_id"]
    assert list(base.keys(base.descriptor(spec))) == list(keys(descriptor(spec)))


def test_family_keys_equal_explicit_keys(family):
    _source, spec, store, ids = family
    loaded = store.registries(spec)
    generated = list(keys(descriptor(spec, loaded), loaded))
    assert len(generated) == len(set(generated)) == 5 + 3 * 8
    assert "A/1/1002_372/0/c/4/0/0" in generated
    assert "B/2/7_1/normalization_stats.json" in generated


def test_family_publish_verify_restore_without_listing(family, tmp_path, monkeypatch):
    source, spec, store, _ids = family
    import os
    for name in ("scandir", "listdir"):
        original = getattr(os, name)

        def guarded(path=".", *args, _original=original, **kwargs):
            if str(path).startswith(str(source)):
                raise AssertionError("source directory listing")
            return _original(path, *args, **kwargs)
        monkeypatch.setattr(os, name, guarded)
    first = publish(store, source, spec, workers=3)
    assert first["files"] == 29 and first["partitions"] <= 8
    assert store.verify(first["release_id"], source)["ok"]
    monkeypatch.undo()
    target = tmp_path / "restored"
    store.restore(first["release_id"], target)
    assert store.verify(first["release_id"], target)["files"] == 29
    root = json.loads((store.path / "releases" / first["release_id"]).read_bytes())
    assert root["descriptor"]["partitions"] == 8
    assert "A/1/1002_372" not in json.dumps(root)


def test_partition_count_is_part_of_identity_and_worker_independent(family, tmp_path):
    source, spec, store, _ids = family
    first = publish(store, source, spec, workers=1)
    other_store = Store(tmp_path / "again")
    other_store.put_registry((store.path / "registries" / spec["families"][0]["registry"]).read_bytes())
    reordered = dict(spec, files=list(reversed(spec["files"])),
                     families=[dict(spec["families"][0], files=list(reversed(spec["families"][0]["files"])))])
    again = publish(other_store, source, reordered, workers=5)
    assert again["release_id"] == first["release_id"]
    wider = publish(other_store, source, dict(spec, partitions=16))
    assert wider["release_id"] != first["release_id"]
    assert sorted(r[:2] for r in store.records(first["release_id"])) == \
        sorted(r[:2] for r in other_store.records(wider["release_id"]))


def test_more_partitions_pass_a_cap_that_64_cannot(tmp_path, monkeypatch):
    monkeypatch.setattr(releases, "MAX_PARTITION_FILES", 3)
    source = tmp_path / "source"
    files = [f"f{i}" for i in range(300)]
    for name in files:
        (source / name).parent.mkdir(parents=True, exist_ok=True)
        (source / name).write_bytes(name.encode())
    with pytest.raises(ValueError, match="file-count limit"):
        publish(Store(tmp_path / "a"), source, {"version": 1, "files": files, "grids": []})
    plan = Store(tmp_path / "b").plan({"version": 2, "partitions": 1024, "files": files, "grids": []})
    assert plan["keys"] == 300 and not plan["exceeds_partition_limit"]
    result = publish(Store(tmp_path / "b"), source, {"version": 2, "partitions": 1024, "files": files, "grids": []})
    assert result["files"] == 300


@pytest.mark.parametrize("change", [
    {"partitions": 0}, {"partitions": 65537}, {"partitions": True}, {"unknown": 1},
])
def test_invalid_version_2_fields_rejected(family, change):
    _source, spec, store, _ids = family
    bad = dict(spec, **change)
    with pytest.raises(ValueError):
        descriptor(bad, store.registries(bad))


@pytest.mark.parametrize("ids,top_files,top_grids", [
    (["a", "a/b"], [], []),
    (["a/b"], ["a/b"], []),
    (["a/b"], ["a/b/zarr.json"], []),
    (["a/b"], ["a"], []),
    (["a/b"], [], [{"prefix": "a", "grid_shape": [2]}]),
    (["a/b"], [], [{"prefix": "a/b/c", "grid_shape": [2]}]),
])
def test_family_overlaps_rejected(tmp_path, ids, top_files, top_grids):
    store = Store(tmp_path / "s")
    value, count = store.put_registry(registry(ids))
    spec = {"version": 2, "files": top_files, "grids": top_grids,
            "families": [{"registry": value, "count": count, "files": ["zarr.json"], "grids": []}]}
    with pytest.raises(ValueError):
        descriptor(spec, store.registries(spec))


def test_family_registry_errors(tmp_path):
    store = Store(tmp_path / "s")
    for data in (b"b\na\n", b"a\na\n", b"a", b"../x\n", b"a//b\n"):
        with pytest.raises(ValueError):
            store.put_registry(data)
    first, _ = store.put_registry(b"a\n")
    second, _ = store.put_registry(b"b\nc\n")
    template = {"files": ["zarr.json"], "grids": []}
    spec = {"version": 2, "files": [], "grids": [], "families": [
        dict(template, registry=first, count=1), dict(template, registry=second, count=2)]}
    assert len(list(keys(descriptor(spec, store.registries(spec)), store.registries(spec)))) == 3
    for bad in (dict(spec, families=[dict(template, registry=first, count=2)]),
                dict(spec, families=[dict(template, registry=first, count=1)] * 2),
                dict(spec, families=[dict(registry=first, count=1, files=[], grids=[])]),
                dict(spec, families=[dict(template, registry="0" * 64, count=1)])):
        with pytest.raises(ValueError):
            descriptor(bad, store.registries(bad))
    overlap, _ = store.put_registry(b"a\nd\n")
    with pytest.raises(ValueError, match="two families"):
        bad = dict(spec, families=[dict(template, registry=first, count=1), dict(template, registry=overlap, count=2)])
        descriptor(bad, store.registries(bad))
    (store.path / "registries" / second).write_bytes(b"b\nz\n")
    with pytest.raises(ValueError, match="corrupt"):
        store.registries(spec)


def test_validation_is_not_all_pairs():
    """The base commit compared every explicit file with every grid: 2e9 checks here."""
    files = [f"g{i}/zarr.json" for i in range(100_000)] + [f"g{i}/stats.json" for i in range(100_000)]
    grids = [{"prefix": f"g{i}/c", "grid_shape": [5, 1]} for i in range(10_000)]
    started = time.monotonic()
    spec = descriptor({"version": 2, "partitions": 512, "files": files, "grids": grids})
    assert time.monotonic() - started < 20
    assert len(spec["files"]) == 200_000
    with pytest.raises(ValueError, match="generated key"):
        descriptor({"version": 2, "files": files + ["g9999/c/4/0"], "grids": grids})


def test_family_validation_scales_linearly(tmp_path):
    store = Store(tmp_path / "s")
    ids = [f"A/{w}/{i}_{j}" for w in range(1, 4) for i in range(100) for j in range(1000)]
    value, count = store.put_registry(registry(ids))
    spec = {"version": 2, "partitions": 512, "files": ["zarr.json"], "grids": [], "families": [
        {"registry": value, "count": count, "files": ["zarr.json", "0/zarr.json"],
         "grids": [{"prefix": "0/c", "grid_shape": [5, 1, 1]}]}]}
    started = time.monotonic()
    plan = store.plan(spec)
    assert time.monotonic() - started < 60
    assert plan["keys"] == 1 + 300_000 * 7
    assert plan["root_bytes_estimate"] < 100_000
    assert not plan["exceeds_partition_limit"]


@pytest.mark.parametrize("mode", ["parent", "full"])
def test_family_delta_matches_fresh_publication(family, tmp_path, mode):
    source, spec, store, _ids = family
    first = publish(store, source, spec)
    changed = ["A/1/1002_373/normalization_stats.json", "B/2/7_1/0/c/3/0/0"]
    for key in changed:
        (source / key).write_bytes(b"revision B " + key.encode())
    second = publish(store, source, spec, source_id="fixed-fixture-B", parent=first["release_id"],
                     changed=changed, assume_complete_changes=True, delta_plan=mode)
    assert second["source_files_read"] == 2 and second["delta_plan"] == mode
    fresh_store = Store(tmp_path / "fresh")
    fresh_store.put_registry((store.path / "registries" / spec["families"][0]["registry"]).read_bytes())
    fresh = publish(fresh_store, source, spec)
    assert second["release_id"] == fresh["release_id"]
    assert store.verify(first["release_id"])["ok"]


def test_parent_plan_delta_generates_no_keys(family, monkeypatch):
    source, spec, store, _ids = family
    first = publish(store, source, spec)
    (source / "zarr.json").write_bytes(b"metadata B")

    def forbidden(*args, **kwargs):
        raise AssertionError("key generation")
    monkeypatch.setattr(releases, "keys", forbidden)
    second = publish(store, source, spec, source_id="B", parent=first["release_id"],
                     changed=["zarr.json"], assume_complete_changes=True)
    assert second["source_files_read"] == 1
    assert second["reused_partitions"] == second["partitions"] - 1
    with pytest.raises(ValueError, match="outside"):
        publish(store, source, spec, source_id="C", parent=first["release_id"],
                changed=["A/1/1002_372/missing.json"], assume_complete_changes=True)


def test_parent_plan_delta_resume(family):
    source, spec, store, _ids = family
    first = publish(store, source, spec)
    changed = sorted(k for k in keys(descriptor(spec, store.registries(spec)), store.registries(spec))
                     if k.endswith("normalization_stats.json"))
    for key in changed:
        (source / key).write_bytes(b"B" + key.encode())

    def stop(count):
        if count == 1:
            raise RuntimeError("interrupted publisher")
    options = dict(source_id="B", parent=first["release_id"], changed=changed, assume_complete_changes=True)
    with pytest.raises(RuntimeError):
        publish(store, source, spec, workers=1, progress=stop, **options)
    resumed = publish(store, source, spec, workers=1, **options)
    assert resumed["reused_partitions"] >= 1
    assert store.verify(resumed["release_id"], source)["ok"]
    assert store.verify(first["release_id"])["ok"]


def test_cli_registry_and_plan(tmp_path, capsys):
    ids = tmp_path / "ids.txt"
    ids.write_bytes(registry(["x/1", "x/2"]))
    assert main(["registry", str(tmp_path / "s"), str(ids)]) == 0
    value = json.loads(capsys.readouterr().out)
    spec = tmp_path / "d.json"
    spec.write_text(json.dumps({"version": 2, "partitions": 4, "files": [], "grids": [], "families": [
        {"registry": value["registry"], "count": value["count"], "files": ["zarr.json"], "grids": []}]}))
    assert main(["plan", str(tmp_path / "s"), str(spec)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["keys"] == 2 and plan["partitions"] == 4


@pytest.mark.parametrize("workers", [1, 4])
def test_parallel_selected_restore_and_verify(family, tmp_path, workers):
    source, spec, store, _ids = family
    release = publish(store, source, spec)["release_id"]
    full = store.verify(release, workers=workers)
    assert full["files"] == 29 and full == store.verify(release)
    target = tmp_path / f"selected-{workers}"
    out = store.restore(release, target, workers=workers, prefixes=["A/1/1002_373", "zarr.json"])
    assert out["files"] == 9
    restored = sorted(str(p.relative_to(target)) for p in target.rglob("*") if p.is_file())
    assert restored == sorted(["zarr.json"] + [f"A/1/1002_373/{k}" for k in
                              ["zarr.json", "normalization_stats.json", "0/zarr.json"] +
                              [f"0/c/{i}/0/0" for i in range(5)]])
    checked = store.verify(release, target, workers=workers, prefixes=["A/1/1002_373", "zarr.json"])
    assert checked["files"] == 9 and checked["scope"] == "selected-prefixes"
    with pytest.raises(OSError):
        store.verify(release, target, workers=workers)
    assert not (tmp_path / "selected-prefix-bad").exists()
    with pytest.raises(ValueError):
        store.restore(release, tmp_path / "selected-prefix-bad", prefixes=["../x"])
    assert not (tmp_path / "selected-prefix-bad").exists()


def test_parallel_restore_failure_leaves_no_destination(family, tmp_path):
    source, spec, store, _ids = family
    release = publish(store, source, spec)["release_id"]
    record = next(store.records(release))
    obj = store.object_path(record[2])
    obj.write_bytes(b"\0" * obj.stat().st_size)
    with pytest.raises(ValueError, match="retained bytes"):
        store.restore(release, tmp_path / "bad", workers=4)
    with pytest.raises(ValueError, match="retained bytes"):
        store.verify(release, workers=4)
    assert not (tmp_path / "bad").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".restore-")]


def test_new_store_fans_out_objects_and_keeps_identity(family, tmp_path):
    source, spec, store, _ids = family
    assert store.fanout
    release = publish(store, source, spec)
    objects = [p for p in (store.path / "objects").rglob("*") if p.is_file()]
    assert objects and all(p.parent.parent.parent == store.path / "objects" and
                           p.parent.name == p.name[2:4] and p.parent.parent.name == p.name[:2] for p in objects)
    flat = Store(tmp_path / "flat")
    (flat.path / "layout").unlink()
    flat = Store(flat.path)
    assert not flat.fanout
    flat.put_registry((store.path / "registries" / spec["families"][0]["registry"]).read_bytes())
    other = publish(flat, source, spec)
    assert other["release_id"] == release["release_id"]
    assert all(p.parent == flat.path / "objects" for p in (flat.path / "objects").iterdir())
    assert flat.verify(other["release_id"])["ok"] and store.verify(release["release_id"])["ok"]
