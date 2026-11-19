"""Check disk-backed graph membership, resource limits, and unchanged identities."""

import json
from pathlib import Path

import pytest

from zarrsum import remotes
from zarrsum.graph_index import Workspace, WithoutRoot, VerifiedKeys
from zarrsum.releases import canonical, digest, descriptor, keys, partition_of


def fixture(count):
    objects, parts = {}, [[] for _ in range(32)]
    for i in range(count):
        raw = str(i).encode()
        value = digest(raw)
        key = f"chunks/{i}"
        objects[remotes.object_key(value)] = raw
        parts[partition_of(key, 32)].append([key, len(raw), value])
    root = {
        "format": remotes.FORMAT,
        "descriptor": {
            "version": 2,
            "partitions": 32,
            "files": [],
            "grids": [{"prefix": "chunks", "grid_shape": [count]}],
        },
        "partitions": [],
    }
    for i, rows in enumerate(parts):
        if not rows:
            continue
        raw = canonical(sorted(rows))
        value = digest(raw)
        objects["manifests/" + value] = raw
        root["partitions"].append([i, value, len(rows), sum(r[1] for r in rows)])
    raw = canonical(root)
    rid = digest(raw)
    objects["releases/" + rid] = raw
    return rid, objects


def test_maps_and_views_do_not_copy_the_graph():
    with_workspace = Workspace()
    try:
        data = with_workspace.table()
        data["root"] = [0, "r"]
        data["a"] = [3, "x"]
        data["a"] = [4, "y"]
        assert len(data) == 2 and list(data.items()) == [
            ("a", [4, "y"]),
            ("root", [0, "r"]),
        ]
        wanted = WithoutRoot(data, "root")
        missing = with_workspace.table()
        assert "a" in VerifiedKeys(wanted, missing)
        missing["a"] = wanted["a"]
        assert "a" not in VerifiedKeys(wanted, missing)
        assert len(VerifiedKeys(wanted, missing)) == 0
        del data["a"]
        assert len(data) == 1
        path = Path(with_workspace.directory.name)
        assert (
            with_workspace.connection.execute("pragma cache_size").fetchone()[0]
            == -8192
        )
        assert with_workspace.connection.execute("pragma mmap_size").fetchone()[0] == 0
    finally:
        with_workspace.close()
    assert not path.exists()


def test_hundred_thousand_file_graph_is_disk_backed_and_complete(tmp_path):
    rid, objects = fixture(100000)
    root, content, records = remotes.graph(rid, objects.__getitem__)
    assert len(records) == 100000 and len(content) == 100033
    assert not isinstance(content, dict) and not isinstance(records, dict)
    assert content.workspace is records.workspace
    assert len(records["chunks/99999"]) == 3
    assert digest(canonical(root)) == rid
    content.workspace.close()


@pytest.mark.parametrize("fault", ["missing", "foreign", "duplicate"])
def test_rehashed_invalid_manifest_still_rejected(fault):
    rid, objects = fixture(25)
    root = json.loads(objects["releases/" + rid])
    entry = next(e for e in root["partitions"] if e[2] > 1)
    rows = json.loads(objects["manifests/" + entry[1]])
    if fault == "missing":
        rows.pop()
    elif fault == "foreign":
        rows[0][0] = "not-declared"
    else:
        rows[1] = rows[0]
    raw = canonical(rows)
    entry[1], entry[2], entry[3] = digest(raw), len(rows), sum(r[1] for r in rows)
    objects["manifests/" + entry[1]] = raw
    raw = canonical(root)
    bad = digest(raw)
    objects["releases/" + bad] = raw
    with pytest.raises(ValueError):
        remotes.graph(bad, objects.__getitem__)


def test_registry_validation_uses_disk_namespace_sets():
    with_workspace = Workspace()
    registries = with_workspace.table(binary=True)
    raw = b"cells/a\ncells/b\n"
    value = digest(raw)
    registries[value] = raw
    spec = {
        "version": 2,
        "partitions": 4,
        "files": [],
        "grids": [],
        "families": [
            {
                "registry": value,
                "count": 2,
                "files": ["meta"],
                "grids": [{"prefix": "c", "grid_shape": [2]}],
            }
        ],
    }
    assert descriptor(spec, {value: raw}) == descriptor(
        spec, registries, index=with_workspace.set
    )
    assert (
        len(
            list(
                keys(descriptor(spec, registries, index=with_workspace.set), registries)
            )
        )
        == 6
    )
    bad_raw = b"cells/a\ncells/a/child\n"
    bad_value = digest(bad_raw)
    registries[bad_value] = bad_raw
    spec["families"][0]["registry"] = bad_value
    with pytest.raises(ValueError, match="overlapping registry ids"):
        descriptor(spec, registries, index=with_workspace.set)
    with_workspace.close()


def test_evidence_is_spooled_and_preview_is_bounded(tmp_path):
    from zarrsum.graph_index import EvidenceLog

    log = EvidenceLog(tmp_path / "commands.jsonl", preview=2)
    for i in range(10):
        log.append(["command", str(i)])
    assert len(log) == 2 and log.count == 10
    assert len(log.path.read_text().splitlines()) == 10
    limited = EvidenceLog(tmp_path / "small.jsonl", byte_limit=1)
    with pytest.raises(ValueError, match="evidence log"):
        limited.append(["large"])


def test_large_fetch_uses_multiple_bounded_plans(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from zarrsum.releases import Store
    from zarrsum.transport import TransferResult

    source = tmp_path / "source"
    source.mkdir()
    for i in range(4):
        (source / str(i)).write_bytes(str(i).encode())
    sender = Store(tmp_path / "sender")
    rid = sender.publish(
        source,
        {
            "version": 2,
            "partitions": 4,
            "files": [str(i) for i in range(4)],
            "grids": [],
        },
        source_id="fixture",
        assume_immutable=True,
    )["release_id"]
    root, content, _ = remotes.graph(
        rid, lambda key: remotes.local_get(sender.path, key)
    )
    data = {key: remotes.local_get(sender.path, key) for key in content}
    content.workspace.close()
    client = SimpleNamespace(
        meta=SimpleNamespace(
            endpoint_url="http://127.0.0.1:13900",
            events=SimpleNamespace(register=lambda *args: None),
        )
    )

    class Transport:
        def copy(self, source, destination, keys):
            for key in keys:
                path = Path(destination) / key
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data[key])
            return TransferResult(True, "transferred")

    monkeypatch.setattr(remotes, "MAX_ENTRIES", 2)
    from zarrsum import transfer_plans

    monkeypatch.setattr(transfer_plans, "MAX_ENTRIES", 2)
    boundary = remotes.Boundary(
        client, tmp_path / "unused", tmp_path / "evidence", transport=Transport()
    )
    boundary.repository = lambda create=False: None
    boundary.get = data.__getitem__
    cache = tmp_path / "cache"
    receipt = boundary.fetch(rid, cache)
    assert receipt["downloaded_objects"] == 4 and receipt["result"] == "verified"
    plan = json.loads((boundary.evidence / "fetch.plan.json").read_bytes())
    assert [count for _, count in plan["plans"]] == [2, 2]
    assert Store(cache).verify(rid)["ok"]


def test_database_byte_budget_is_enforced(monkeypatch):
    import sqlite3
    from zarrsum import graph_index

    monkeypatch.setattr(graph_index, "SCRATCH_BYTES", 32 << 10)
    workspace = Workspace()
    try:
        mapping = workspace.table(binary=True)
        with pytest.raises(sqlite3.DatabaseError, match="full"):
            for i in range(100):
                mapping[str(i)] = b"x" * 8192
    finally:
        workspace.close()
