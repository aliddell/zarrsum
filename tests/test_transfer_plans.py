"""Check complete coverage, bounded plans, and publication across plan boundaries."""

import json
from types import SimpleNamespace

import pytest

from zarrsum import remotes, transfer_plans as plans
from zarrsum.releases import Store, canonical, digest
from zarrsum.transport import TransferResult

RID = "a" * 64
SOURCE = "/private/sender"
DESTINATION = "s3:test/retained"
REPOSITORY = {
    "endpoint": "http://127.0.0.1:13900",
    "bucket": "test",
    "prefix": "retained",
}


def expected(count):
    return {f"objects/{i:05d}": [1, digest(str(i).encode())] for i in range(count)}


def write_index(tmp_path, count=10017):
    path = tmp_path / "push.plan.json"
    entries = expected(count)
    pin = plans.create_index(path, RID, SOURCE, DESTINATION, REPOSITORY, entries)
    return path, pin, entries


def check(path, pin, entries):
    return plans.validate_index(
        path, pin, RID, SOURCE, DESTINATION, REPOSITORY, entries
    )


def rewrite_child(path, edit):
    document = json.loads(path.read_bytes())
    child = path.parent / "plans" / (document["plans"][0][0] + ".json")
    value = json.loads(child.read_bytes())
    edit(value)
    raw = canonical(value)
    pin = digest(raw)
    (child.parent / (pin + ".json")).write_bytes(raw)
    document["plans"][0] = [pin, len(value["entries"])]
    raw = canonical(document)
    path.write_bytes(raw)
    return digest(raw)


def test_real_plan_boundary_preserves_child_bound(tmp_path):
    path, pin, entries = write_index(tmp_path)
    children = check(path, pin, entries)
    assert [len(json.loads(p.read_bytes())["entries"]) for p, _ in children] == [
        10000,
        17,
    ]
    assert remotes.MAX_FILES == 10000
    assert digest(path.read_bytes()) == pin


@pytest.mark.parametrize(
    "fault",
    ["omit", "duplicate", "foreign", "wrong-size", "root", "wrong-source", "oversized"],
)
def test_rehashed_invalid_child_rejected_before_execution(tmp_path, fault):
    path, pin, entries = write_index(tmp_path, 3)

    def edit(child):
        if fault == "omit":
            child["entries"].pop()
        elif fault == "duplicate":
            child["entries"].append(child["entries"][0])
        elif fault == "foreign":
            child["entries"][0][0] = "objects/not-in-graph"
        elif fault == "wrong-size":
            child["entries"][0][1] += 1
        elif fault == "root":
            child["entries"][0][0] = "releases/" + RID
        elif fault == "wrong-source":
            child["source"] = "/other/source"
        else:
            child["entries"] *= 4000

    pin = rewrite_child(path, edit)
    with pytest.raises(ValueError):
        check(path, pin, entries)


@pytest.mark.parametrize(
    "fault",
    ["count", "duplicate-child", "reordered", "repository", "too-many", "unsafe-child"],
)
def test_invalid_index_rejected(tmp_path, monkeypatch, fault):
    monkeypatch.setattr(plans, "MAX_ENTRIES", 2)
    path, pin, entries = write_index(tmp_path, 3)
    document = json.loads(path.read_bytes())
    if fault == "count":
        document["entries_count"] += 1
    elif fault == "duplicate-child":
        document["plans"].append(document["plans"][0])
    elif fault == "reordered":
        document["plans"].reverse()
    elif fault == "repository":
        document["repository"]["endpoint"] = "http://127.0.0.2:13900"
    elif fault == "too-many":
        document["plans"] *= 65
    else:
        document["plans"][0][0] = "../escape"
    raw = canonical(document)
    path.write_bytes(raw)
    with pytest.raises(ValueError):
        check(path, digest(raw), entries)


def test_missing_mutated_and_oversized_documents(tmp_path, monkeypatch):
    path, pin, entries = write_index(tmp_path, 3)
    child, _ = check(path, pin, entries)[0]
    raw = child.read_bytes()
    child.unlink()
    with pytest.raises(FileNotFoundError):
        check(path, pin, entries)
    child.write_bytes(raw + b" ")
    with pytest.raises(ValueError, match="changed"):
        check(path, pin, entries)
    child.write_bytes(raw)
    monkeypatch.setattr(plans, "PLAN_BYTES", 1)
    with pytest.raises(ValueError, match="byte limit"):
        check(path, pin, entries)
    monkeypatch.setattr(plans, "INDEX_BYTES", 1)
    with pytest.raises(ValueError, match="byte limit"):
        check(path, pin, entries)


def boundary_fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(plans, "MAX_ENTRIES", 1)
    monkeypatch.setattr(remotes, "MAX_ENTRIES", 1)
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"correct")
    store = Store(tmp_path / "store")
    rid = store.publish(
        source,
        {"version": 2, "partitions": 4, "files": ["data"], "grids": []},
        source_id="fixture",
        assume_immutable=True,
    )["release_id"]
    objects, copied = {}, []
    events = SimpleNamespace(register=lambda *args: None)
    client = SimpleNamespace(
        meta=SimpleNamespace(endpoint_url=REPOSITORY["endpoint"], events=events)
    )

    class Transport:
        after_copy = None

        def copy(self, source, destination, keys):
            copied.extend(keys)
            for key in keys:
                objects[key] = (store.path / key).read_bytes()
            assert "releases/" + rid not in objects
            if self.after_copy is not None:
                self.after_copy()
            return TransferResult(True, "transferred")

    transport = Transport()
    boundary = remotes.Boundary(
        client, tmp_path / "unused", tmp_path / "evidence", transport=transport
    )
    boundary.repository = lambda create=False: None
    boundary.maybe = lambda key: objects.get(key)
    boundary.get = lambda key: objects[key]
    boundary.conditional = lambda key, data: objects.setdefault(key, data) and "created"
    return boundary, store, rid, objects, copied, transport


def test_indexed_push_resumes_across_child_plans_and_publishes_last(
    tmp_path, monkeypatch
):
    boundary, store, rid, objects, copied, transport = boundary_fixture(
        tmp_path, monkeypatch
    )
    with pytest.raises(InterruptedError):
        boundary.push(store.path, rid, interrupt_after=1)
    assert len(copied) == 1 and "releases/" + rid not in objects
    path = boundary.evidence / "push.plan.json"
    pin = digest(path.read_bytes())
    first = copied[0]
    result = boundary.push(
        store.path, rid, name="retry", resume_plan=path, plan_digest=pin
    )
    assert result["plan_count"] == 2 and result["plan_version"] == 2
    assert copied.count(first) == 1 and result["reused_keys"] == 1
    assert result["plan_blake3"] == pin and digest(path.read_bytes()) == pin
    assert objects["releases/" + rid] == (store.path / "releases" / rid).read_bytes()


@pytest.mark.parametrize(
    "fault", ["index-mutation", "child-mutation", "transfer-failure"]
)
def test_mutation_or_failure_between_plans_prevents_publication(
    tmp_path, monkeypatch, fault
):
    boundary, store, rid, objects, copied, transport = boundary_fixture(
        tmp_path, monkeypatch
    )

    def fail():
        path = boundary.evidence / "push.plan.json"
        if fault == "index-mutation":
            path.write_bytes(path.read_bytes() + b" ")
        elif fault == "child-mutation":
            document = json.loads(path.read_bytes())
            child = path.parent / "plans" / (document["plans"][1][0] + ".json")
            child.write_bytes(child.read_bytes() + b" ")
        else:
            raise ValueError("injected transfer failure")

    transport.after_copy = fail
    with pytest.raises(ValueError):
        boundary.push(store.path, rid)
    assert len(copied) == 1 and "releases/" + rid not in objects
    assert not (boundary.evidence / "push.receipt.json").exists()


def test_corrupt_partial_object_blocks_indexed_retry(tmp_path, monkeypatch):
    boundary, store, rid, objects, copied, _ = boundary_fixture(tmp_path, monkeypatch)
    with pytest.raises(InterruptedError):
        boundary.push(store.path, rid, interrupt_after=1)
    path = boundary.evidence / "push.plan.json"
    pin = digest(path.read_bytes())
    key = copied[0]
    objects[key] = b"x" * len(objects[key])
    with pytest.raises(ValueError, match="corrupt remote object"):
        boundary.push(store.path, rid, name="retry", resume_plan=path, plan_digest=pin)
    assert "releases/" + rid not in objects
    assert len(copied) == 1


def test_mutation_during_target_check_blocks_root(tmp_path, monkeypatch):
    boundary, store, rid, objects, copied, _ = boundary_fixture(tmp_path, monkeypatch)

    def read(key):
        if len(copied) == 2:
            path = boundary.evidence / "push.plan.json"
            path.write_bytes(path.read_bytes() + b" ")
        return objects[key]

    boundary.get = read
    with pytest.raises(ValueError, match="changed"):
        boundary.push(store.path, rid)
    assert "releases/" + rid not in objects


def test_oversized_graph_still_rejected(tmp_path):
    spec = {
        "version": 2,
        "partitions": 4,
        "files": [],
        "grids": [{"prefix": "chunks", "grid_shape": [10001]}],
    }
    root = {"format": remotes.FORMAT, "descriptor": spec, "partitions": []}
    raw = canonical(root)
    with pytest.raises(ValueError, match="oversized graph"):
        remotes.graph(digest(raw), lambda key: raw)


def test_ten_thousand_file_graph_push_crosses_old_limit(tmp_path, monkeypatch):
    """Keep payloads in memory so this boundary test creates only plan artifacts."""
    from zarrsum.releases import partition_of

    payloads, partitions = {}, [[] for _ in range(16)]
    for i in range(10000):
        raw = str(i).encode()
        value = digest(raw)
        key = f"chunks/{i}"
        payloads[remotes.object_key(value)] = raw
        partitions[partition_of(key, 16)].append([key, len(raw), value])
    root = {
        "format": remotes.FORMAT,
        "descriptor": {
            "version": 2,
            "partitions": 16,
            "files": [],
            "grids": [{"prefix": "chunks", "grid_shape": [10000]}],
        },
        "partitions": [],
    }
    for i, rows in enumerate(partitions):
        raw = canonical(sorted(rows))
        value = digest(raw)
        payloads["manifests/" + value] = raw
        root["partitions"].append([i, value, len(rows), sum(r[1] for r in rows)])
    raw = canonical(root)
    rid = digest(raw)
    payloads["releases/" + rid] = raw
    monkeypatch.setattr(remotes, "local_get", lambda directory, key: payloads[key])
    objects, calls = {}, []
    events = SimpleNamespace(register=lambda *args: None)
    client = SimpleNamespace(
        meta=SimpleNamespace(endpoint_url=REPOSITORY["endpoint"], events=events)
    )

    class Transport:
        def copy(self, source, destination, keys):
            assert len(keys) <= 64
            assert "releases/" + rid not in objects
            calls.extend(keys)
            objects.update((key, payloads[key]) for key in keys)
            return TransferResult(True, "transferred")

    boundary = remotes.Boundary(
        client, tmp_path / "unused", tmp_path / "evidence", transport=Transport()
    )
    boundary.repository = lambda create=False: None
    boundary.maybe = lambda key: objects.get(key)
    boundary.get = lambda key: objects[key]
    boundary.conditional = lambda key, data: objects.setdefault(key, data) and "created"
    receipt = boundary.push(tmp_path, rid)
    assert receipt["files"] == 10000 and receipt["uploaded_keys"] == 10016
    assert receipt["plan_count"] == 2 and receipt["result"] == "verified"
    assert len(calls) == len(set(calls)) == 10016
    assert objects["releases/" + rid] == raw
