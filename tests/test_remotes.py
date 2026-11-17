"""Validate untrusted release metadata before it can supply local paths."""

import pytest
from zarrsum.releases import Store, canonical, digest
from zarrsum.remotes import graph, local_get, Boundary


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


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://8.8.8.8:13900",
        "http://0.0.0.0:13900",
        "http://10.1.2.3:80",
        "http://10.1.2.3:13900/path",
        "http://user@10.1.2.3:13900",
        "https://10.1.2.3:13900",
    ],
)
def test_fixture_endpoint_rejects_unapproved_forms(endpoint):
    from zarrsum.remotes import fixture_endpoint

    with pytest.raises(ValueError):
        fixture_endpoint(endpoint, {endpoint})


def test_fixture_endpoint_requires_explicit_match():
    from zarrsum.remotes import fixture_endpoint

    allowed = {"http://10.1.2.3:13900"}
    assert fixture_endpoint("http://10.1.2.3:13900", allowed) == "http://10.1.2.3:13900"
    with pytest.raises(ValueError):
        fixture_endpoint("http://10.1.2.4:13900", allowed)


def test_rclone_endpoint_must_match_sdk(tmp_path):
    class FakeClient:
        class meta:
            endpoint_url = "http://10.1.2.3:13900"

    config = tmp_path / "rclone.conf"
    config.write_text(
        "[s3]\nendpoint=http://10.1.2.4:13900\nregion=garage\nenv_auth=false\nforce_path_style=true\n"
    )
    with pytest.raises(ValueError, match="rclone configuration"):
        Boundary(
            FakeClient(),
            config,
            tmp_path / "evidence",
            allowed_endpoints={FakeClient.meta.endpoint_url},
        )


def test_plan_mutation_before_retry_blocks_transport(tmp_path):
    store, rid = release(tmp_path)

    class FakeClient:
        class meta:
            endpoint_url = "http://127.0.0.1:13900"

            class events:
                @staticmethod
                def register(*args):
                    pass

    class InterruptedTransport:
        calls = 0

        def copy(self, *args):
            self.calls += 1
            raise InterruptedError("test interruption")

    transport = InterruptedTransport()
    boundary = Boundary(
        FakeClient(), tmp_path / "unused", tmp_path / "plans", transport=transport
    )
    path, pin = boundary.plan(
        rid, str(store.path), "target", [["data", 3, "0" * 64]], "retry"
    )
    with pytest.raises(InterruptedError):
        boundary.execute(path, pin)
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="transfer plan changed"):
        boundary.execute(path, pin)
    assert transport.calls == 1
