"""Check identity, location fallback, admission, and job-facing preparation receipts."""

import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from zarrsum import locations
from zarrsum.locations import CacheError, filesystem_root, location, prepare, read_json
from zarrsum.releases import Store, canonical
from zarrsum.remotes import REPOSITORY, Aliases, graph, local_get
from zarrsum.transport import TransferResult


@pytest.fixture
def published(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "metadata.json").write_bytes(b'{"scale":1}')
    store = Store(tmp_path / "publisher")
    rid, count = store.put_registry(b"images/a\nimages/b\n")
    spec = {
        "version": 2,
        "partitions": 4,
        "files": ["metadata.json"],
        "grids": [],
        "families": [
            {"registry": rid, "count": count, "files": ["value"], "grids": []}
        ],
    }
    for name in ["a", "b"]:
        path = source / "images" / name / "value"
        path.parent.mkdir(parents=True)
        path.write_bytes(("pixels-" + name).encode())
    rid = store.publish(source, spec, source_id="fixture", assume_immutable=True)[
        "release_id"
    ]
    ref = {"version": 1, "release_id": rid, "locations": [fs_location(store.path)]}
    return SimpleNamespace(store=store, rid=rid, ref=ref, source=source)


def fs_location(path):
    return {"storage_platform": "sf_hpc", "location_uri": "sf_hpc://" + str(path)}


def run(published, tmp_path, **kwargs):
    return prepare(
        published.ref,
        tmp_path / "cache",
        tmp_path / "checkout",
        tmp_path / "evidence",
        local_platform="sf_hpc",
        **kwargs,
    )


def test_local_prepare_returns_verified_paths_and_provenance(published, tmp_path):
    result = run(published, tmp_path)
    assert result["release_id"] == published.rid and not result["cache_hit"]
    assert result["selected_location"] == published.ref["locations"][0]
    assert result["checkout"]["files"] == 3 and result["checkout"]["ok"]
    assert (Path(result["destination"]) / "images/a/value").read_bytes() == b"pixels-a"
    assert result["fetch"]["physical_root"] == str(published.store.path)
    assert result["reference"] == published.ref
    assert result["local_platform"] == "sf_hpc"
    assert result["max_fetch_bytes"] == locations.MAX_FETCH_BYTES
    assert result == json.loads(
        (tmp_path / "evidence/prepare.receipt.json").read_bytes()
    )
    assert Store(tmp_path / "cache").verify(published.rid)["ok"]


def test_cache_hit_needs_no_location_or_provider_configuration(
    published, tmp_path, monkeypatch
):
    run(published, tmp_path)
    shutil.rmtree(published.store.path)
    monkeypatch.setattr(
        locations, "_s3_fetch", lambda *a: pytest.fail("accessed a remote")
    )
    ref = {
        "version": 1,
        "release_id": published.rid,
        "locations": [
            {"storage_platform": "s3", "location_uri": "s3://missing-bucket/store"}
        ],
    }
    result = prepare(
        ref,
        tmp_path / "cache",
        tmp_path / "offline",
        tmp_path / "offline-evidence",
        remotes=tmp_path / "absent-config",
        location_remotes=tmp_path / "absent-access",
    )
    assert (
        result["cache_hit"]
        and result["selected_location"] is None
        and not result["attempts"]
    )


def test_another_store_recovers_after_original_store_and_source_disappear(
    published, tmp_path
):
    first = run(published, tmp_path)
    shutil.rmtree(published.store.path)
    shutil.rmtree(published.source)
    ref = {
        "version": 1,
        "release_id": published.rid,
        "locations": [fs_location(tmp_path / "cache")],
    }
    reference = tmp_path / "relocated.json"
    reference.write_bytes(canonical(ref))
    command = [
        sys.executable,
        "-m",
        "zarrsum.releases",
        "prepare",
        str(reference),
        str(tmp_path / "fresh-cache"),
        str(tmp_path / "fresh-checkout"),
        "--local-platform",
        "sf_hpc",
        "--evidence",
        str(tmp_path / "fresh-evidence"),
    ]
    process = subprocess.run(command, capture_output=True, text=True)
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert result["release_id"] == first["release_id"]
    assert (Path(result["destination"]) / "images/b/value").read_bytes() == b"pixels-b"


@pytest.mark.parametrize(
    "fault",
    ["missing", "incomplete", "corrupt", "metadata", "permission", "wrong-platform"],
)
def test_bad_candidate_falls_back_without_changing_identity(
    published, tmp_path, monkeypatch, fault
):
    broken = tmp_path / "bad-store"
    shutil.copytree(published.store.path, broken)
    if fault in ["incomplete", "corrupt"]:
        row = next(published.store.records(published.rid))
        path = Store(broken).object_path(row[2])
        raw = path.read_bytes()
        if fault == "incomplete":
            path.unlink()
        else:
            path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    if fault == "metadata":
        path = broken / "releases" / published.rid
        path.write_bytes(path.read_bytes() + b" ")
    if fault == "missing":
        shutil.rmtree(broken)
    if fault == "permission":
        original = locations.local_get

        def denied(root, key):
            if Path(root) == broken:
                raise PermissionError("test")
            return original(root, key)

        monkeypatch.setattr(locations, "local_get", denied)
    bad = fs_location(broken)
    if fault == "wrong-platform":
        bad = {"storage_platform": "reef", "location_uri": "reef://unreachable-store"}
    published.ref["locations"].insert(0, bad)
    result = run(published, tmp_path)
    assert [attempt["result"] for attempt in result["attempts"]] == [
        "refused",
        "verified",
    ]
    assert result["release_id"] == published.rid
    assert result["selected_location"] == fs_location(published.store.path)


def test_only_corrupt_candidate_publishes_no_root_checkout_or_receipt(
    published, tmp_path
):
    path = published.store.object_path(next(published.store.records(published.rid))[2])
    raw = path.read_bytes()
    path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    with pytest.raises(ValueError, match="no candidate"):
        run(published, tmp_path)
    assert not (tmp_path / "checkout").exists()
    assert not (tmp_path / "cache/releases" / published.rid).exists()
    assert not (tmp_path / "evidence/prepare.receipt.json").exists()
    assert not list(tmp_path.glob(".prepare-*"))


def test_corrupt_cache_is_not_silently_repaired(published, tmp_path):
    run(published, tmp_path)
    cache = Store(tmp_path / "cache")
    path = cache.object_path(next(cache.records(published.rid))[2])
    raw = path.read_bytes()
    path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    with pytest.raises(CacheError):
        prepare(
            published.ref,
            cache.path,
            tmp_path / "refused",
            tmp_path / "refused-evidence",
        )
    assert path.read_bytes() != raw and not (tmp_path / "refused").exists()


@pytest.mark.parametrize("symlink", [False, True])
def test_existing_destination_is_preserved_before_cache_access(
    published, tmp_path, symlink
):
    destination = tmp_path / "checkout"
    if symlink:
        destination.symlink_to(tmp_path / "absent-target")
    else:
        destination.mkdir()
        (destination / "sentinel").write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        run(published, tmp_path)
    assert not (tmp_path / "cache").exists()
    if symlink:
        assert destination.is_symlink() and not (tmp_path / "absent-target").exists()
    else:
        assert (destination / "sentinel").read_bytes() == b"keep"


def test_new_content_budget_is_checked_before_copy(published, tmp_path):
    with pytest.raises(ValueError, match="no candidate"):
        run(published, tmp_path, max_fetch_bytes=0)
    assert not (tmp_path / "cache/releases" / published.rid).exists()
    assert not (tmp_path / "checkout").exists()


@pytest.mark.parametrize(
    "platform,uri",
    [
        ("reef", "s3://bucket/root"),
        ("reef", "reef://user:secret@store"),
        ("s3", "s3://user:secret@bucket/root"),
        ("reef", "reef:///tmp/not-shared"),
        ("reef", "reef://../../../tmp"),
        ("sf_hpc", "sf_hpc://relative/store"),
        ("s3", "s3://bucket/root?secret=value"),
        ("s3", "s3://bucket/../escape"),
        ("other", "other:///store"),
        ([], "reef://root"),
    ],
)
def test_invalid_or_unsupported_locations_are_rejected(platform, uri):
    with pytest.raises(ValueError):
        location({"storage_platform": platform, "location_uri": uri})


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("reef://dataset", "/mnt/main0/projects/dataset"),
    ],
)
def test_reef_mapping_uses_the_projects_root(uri, expected):
    assert (
        str(filesystem_root({"storage_platform": "reef", "location_uri": uri}, "reef"))
        == expected
    )


def test_reference_duplicate_json_and_size_limits(tmp_path):
    path = tmp_path / "reference"
    path.write_bytes(b'{"version":1,"version":1}')
    with pytest.raises(ValueError, match="duplicate JSON"):
        read_json(path)
    path.write_bytes(b" " * (locations.REFERENCE_BYTES + 1))
    with pytest.raises(ValueError, match="byte limit"):
        read_json(path)


def s3_setup(published, tmp_path, monkeypatch, *, corrupt=False):
    import zarrsum.remote_cli

    _, content, _ = graph(
        published.rid, lambda key: local_get(published.store.path, key)
    )
    objects = {key: local_get(published.store.path, key) for key in content}
    objects["repository.json"] = canonical(REPOSITORY)
    if corrupt:
        key = next(key for key in content if key.startswith("objects/"))
        raw = objects[key]
        objects[key] = bytes([raw[0] ^ 1]) + raw[1:]

    class Client:
        meta = SimpleNamespace(
            endpoint_url="http://127.0.0.1:13900",
            events=SimpleNamespace(register=lambda *a: None),
        )

        def get_object(self, Bucket, Key):
            key = Key.removeprefix("retained/")
            if key not in objects:
                raise ClientError(
                    {"ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject"
                )
            return {"Body": io.BytesIO(objects[key])}

    class Transport:
        commands = []

        def version(self):
            return "test transport"

        def copy(self, source, destination, keys):
            for key in keys:
                path = Path(destination) / key
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(objects[key])
            return TransferResult(True, "transferred")

    monkeypatch.setattr(
        zarrsum.remote_cli,
        "connection",
        lambda *a, **k: (Client(), Transport(), "unused"),
    )
    aliases = tmp_path / "remotes.json"
    config = {
        "endpoint": "http://127.0.0.1:13900",
        "bucket": "release-test",
        "prefix": "retained",
        "region": "garage",
        "addressing_style": "path",
        "profile": "fixture",
        "rclone_remote": "s3",
        "rclone_config": "unused",
        "format": REPOSITORY["format"],
    }
    Aliases(aliases).add("publisher", config)
    uri = "s3://release-test/retained"
    access = tmp_path / "access.json"
    access.write_bytes(canonical({uri: "publisher"}))
    published.ref["locations"] = [{"storage_platform": "s3", "location_uri": uri}]
    return {"remotes": aliases, "location_remotes": access}


def test_s3_resolves_through_existing_boundary(published, tmp_path, monkeypatch):
    config = s3_setup(published, tmp_path, monkeypatch)
    result = run(published, tmp_path, **config)
    assert (
        result["checkout"]["ok"]
        and result["selected_location"]["storage_platform"] == "s3"
    )
    assert result["fetch"]["access_alias"] == "publisher"


def test_corrupt_s3_bytes_are_refused_before_checkout(published, tmp_path, monkeypatch):
    config = s3_setup(published, tmp_path, monkeypatch, corrupt=True)
    with pytest.raises(ValueError, match="no candidate"):
        run(published, tmp_path, **config)
    assert not (tmp_path / "checkout").exists()
    assert not (tmp_path / "evidence/prepare.receipt.json").exists()
    assert (tmp_path / "evidence/attempt-0000/transport.json").exists()


def test_s3_profile_cannot_redirect_a_location(published, tmp_path, monkeypatch):
    config = s3_setup(published, tmp_path, monkeypatch)
    published.ref["locations"][0]["location_uri"] = "s3://another-bucket/retained"
    Path(config["location_remotes"]).write_bytes(
        canonical({"s3://another-bucket/retained": "publisher"})
    )
    with pytest.raises(ValueError, match="no candidate"):
        run(published, tmp_path, **config)
    assert not (tmp_path / "checkout").exists()


def test_missing_s3_access_can_fall_back_to_a_filesystem(published, tmp_path):
    published.ref["locations"].insert(
        0, {"storage_platform": "s3", "location_uri": "s3://bucket/store"}
    )
    result = run(published, tmp_path)
    assert result["selected_location"]["storage_platform"] == "sf_hpc"
    assert len(result["attempts"]) == 2


def test_missing_s3_configuration_files_do_not_block_other_locations(
    published, tmp_path
):
    published.ref["locations"].insert(
        0, {"storage_platform": "s3", "location_uri": "s3://bucket/store"}
    )
    result = run(
        published,
        tmp_path,
        remotes=tmp_path / "missing-remotes",
        location_remotes=tmp_path / "missing-access",
    )
    assert result["selected_location"]["storage_platform"] == "sf_hpc"
    assert len(result["attempts"]) == 2


def test_interrupted_admission_leaves_no_root_and_retries_verified_partial_bytes(
    published, tmp_path, monkeypatch
):
    cache = tmp_path / "cache"
    original = locations.atomic_file
    interrupted = False

    def stop_after_one_admission(path, data):
        nonlocal interrupted
        original(path, data)
        if Path(path).is_relative_to(cache / "objects") and not interrupted:
            interrupted = True
            raise KeyboardInterrupt("test interruption after admitting verified bytes")

    monkeypatch.setattr(locations, "atomic_file", stop_after_one_admission)
    with pytest.raises(KeyboardInterrupt):
        run(published, tmp_path)
    assert interrupted
    assert not (cache / "releases" / published.rid).exists()
    assert not (tmp_path / "checkout").exists()
    assert not (tmp_path / "evidence/prepare.receipt.json").exists()
    assert not list(tmp_path.glob(".prepare-*"))
    monkeypatch.setattr(locations, "atomic_file", original)
    result = prepare(
        published.ref,
        cache,
        tmp_path / "recovered",
        tmp_path / "retry-evidence",
        local_platform="sf_hpc",
    )
    assert result["fetch"]["cache_hits"] > 0 and result["checkout"]["ok"]


def test_a_damaged_partial_cache_requires_repair_before_retry(published, tmp_path):
    cache = Store(tmp_path / "cache")
    row = next(published.store.records(published.rid))
    path = cache.object_path(row[2])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"corrupt")
    with pytest.raises(CacheError):
        run(published, tmp_path)
    assert not (tmp_path / "checkout").exists()
    assert path.read_bytes() == b"corrupt"


def test_large_missing_inventory_bounds_python_allocation(tmp_path):
    import tracemalloc
    from zarrsum.graph_index import Workspace

    class Content:
        def items(self):
            for number in range(100000):
                value = f"{number:064x}"
                yield f"objects/{value[:2]}/{value[2:4]}/{value}", [1, value]

    workspace = Workspace()
    scratch = Path(workspace.directory.name)
    try:
        tracemalloc.start()
        missing = locations._missing(Content(), tmp_path / "empty-cache", workspace)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert len(missing) == 100000
        assert peak < 4 << 20
        assert missing.workspace is workspace
        assert sum(value[0] for value in missing.values()) == 100000
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        workspace.close()
    assert not scratch.exists()


def test_workspace_cleanup_after_candidate_failure(published, tmp_path, monkeypatch):
    from zarrsum.graph_index import Workspace

    paths = []

    class ObservedWorkspace(Workspace):
        def __init__(self):
            super().__init__()
            paths.append(Path(self.directory.name))

    monkeypatch.setattr(locations, "Workspace", ObservedWorkspace)
    published.ref["locations"] = [fs_location(tmp_path / "missing-store")]
    with pytest.raises(ValueError, match="no candidate"):
        run(published, tmp_path)
    assert paths and not any(path.exists() for path in paths)
    assert not (tmp_path / "checkout").exists()


@pytest.mark.parametrize("uri", ["reef:///mnt/main0/home/user/store", "reef:///bio/projects/data", "reef:///mnt/main0/projects/data"])
def test_reef_absolute_paths_remain_unsupported(uri):
    with pytest.raises(ValueError, match="projects-relative"):
        location({"storage_platform": "reef", "location_uri": uri})


def test_reef_symlink_cannot_escape_projects(monkeypatch):
    monkeypatch.setattr(Path, "resolve", lambda self: Path("/mnt/main0/home/user/store"))
    with pytest.raises(ValueError, match="projects root"):
        filesystem_root({"storage_platform": "reef", "location_uri": "reef://fixture/store"}, "reef")


@pytest.mark.parametrize("batch", [0, 1025, True, 1.5])
def test_prepare_rejects_invalid_batch_before_creating_paths(tmp_path, batch):
    with pytest.raises(ValueError, match="batch size"):
        prepare({}, tmp_path/"cache", tmp_path/"checkout", tmp_path/"evidence", transfer_batch_size=batch)
    assert not list(tmp_path.iterdir())


def test_s3_prepare_forwards_batch_to_boundary_not_connection(tmp_path, monkeypatch):
    mapping = tmp_path / "mapping.json"
    mapping.write_text(json.dumps({"s3://bucket/root": "fixture"}))
    monkeypatch.setattr(locations, "Aliases", lambda path: SimpleNamespace(
        list=lambda: {"fixture": {"bucket": "bucket", "prefix": "root", "rclone_remote": "s3"}}))
    monkeypatch.setattr(locations, "remote_config", lambda value: value)
    from zarrsum import remote_cli

    seen = {}

    def connection(config, **options):
        assert "transfer_batch_size" not in options
        return object(), object(), "rclone.conf"

    def boundary(*args, **kwargs):
        seen.update(kwargs)
        raise RuntimeError("boundary captured")

    monkeypatch.setattr(remote_cli, "connection", connection)
    monkeypatch.setattr(locations, "Boundary", boundary)
    with pytest.raises(RuntimeError, match="boundary captured"):
        locations._s3_fetch({"location_uri": "s3://bucket/root"}, "0"*64,
            tmp_path/"cache", tmp_path/"evidence", tmp_path/"remotes", mapping,
            1024, {"transfer_batch_size": 1024})
    assert seen["batch_size"] == 1024
