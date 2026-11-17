"""Validate untrusted release metadata before it can supply local paths."""

import pytest
from zarrsum.releases import Store, canonical, digest
from zarrsum.remotes import graph, local_get, Aliases, Boundary


def release(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"abc")
    store = Store(tmp_path / "store")
    published = store.publish(
        source,
        {"version": 2, "partitions": 4, "files": ["data"], "grids": []},
        source_id="test",
        assume_immutable=True,
    )
    return store, published["release_id"]


def test_metadata_hash_checked_before_parsing(tmp_path):
    store, rid = release(tmp_path)
    raw = (store.path / "releases" / rid).read_bytes()
    (store.path / "releases" / rid).write_bytes(raw + b" ")
    with pytest.raises(ValueError, match="metadata bytes mismatch"):
        graph(rid, lambda key: local_get(store.path, key))


def test_rejects_unsafe_root_even_when_digest_is_valid(tmp_path):
    store, rid = release(tmp_path)
    root = store._load("releases", rid)
    root["descriptor"]["files"].append("../escape")
    raw = canonical(root)
    bad = digest(raw)
    (store.path / "releases" / bad).write_bytes(raw)
    with pytest.raises(ValueError, match="unsafe member path"):
        graph(bad, lambda key: local_get(store.path, key))


def test_rejects_duplicate_partitions(tmp_path):
    store, rid = release(tmp_path)
    root = store._load("releases", rid)
    root["partitions"] *= 2
    raw = canonical(root)
    bad = digest(raw)
    (store.path / "releases" / bad).write_bytes(raw)
    with pytest.raises(ValueError, match="invalid partition values"):
        graph(bad, lambda key: local_get(store.path, key))


def test_rejects_incomplete_manifest_graph(tmp_path):
    store, rid = release(tmp_path)
    root = store._load("releases", rid)
    root["partitions"] = []
    raw = canonical(root)
    bad = digest(raw)
    (store.path / "releases" / bad).write_bytes(raw)
    with pytest.raises(ValueError, match="incomplete graph"):
        graph(bad, lambda key: local_get(store.path, key))


def test_plan_digest_survives_restart(tmp_path):
    store, rid = release(tmp_path)

    class FakeClient:
        class meta:
            endpoint_url = "http://127.0.0.1:13900"

            class events:
                @staticmethod
                def register(*args):
                    pass

    boundary = Boundary(FakeClient(), tmp_path / "unused-config", tmp_path / "evidence")
    plan, pin = boundary.plan(rid, str(store.path), "s3:release-test/unit", [], "plan")
    plan.write_bytes(plan.read_bytes() + b" ")
    fresh = Boundary(FakeClient(), tmp_path / "unused-config", tmp_path / "evidence")
    with pytest.raises(ValueError, match="transfer plan changed"):
        fresh.execute(plan, pin)
