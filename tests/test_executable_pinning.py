"""Check executable identity before native work and in real transfer receipts."""

import hashlib
import json
from pathlib import Path
import shutil
import sys

import boto3
from botocore.config import Config
import pytest

from test_ozx import local_s3 as local_s3
from zarrsum.locations import prepare
from zarrsum.location_cli import main as prepare_main
from zarrsum.releases import Store, canonical
from zarrsum.remote_cli import connection, main
from zarrsum.remotes import REPOSITORY
from zarrsum.transport import RcloneTransport


def fake(tmp_path, name="native"):
    path = tmp_path / name
    path.write_text(
        "#!"
        + sys.executable
        + "\n"
        + "import sys\nfrom pathlib import Path\n"
        + "Path("
        + repr(str(tmp_path / "executed"))
        + ").write_text('yes')\n"
        + "if 'version' in sys.argv: print('rclone fixture')\n"
    )
    path.chmod(0o755)
    return path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_wrong_pin_does_not_execute_even_a_probe(tmp_path):
    binary = fake(tmp_path)
    transport = RcloneTransport(binary, expected_sha256="0" * 64)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        transport.copy(str(tmp_path / "source"), str(tmp_path / "destination"), ["key"])
    assert not (tmp_path / "executed").exists()
    assert not (tmp_path / "destination").exists()
    assert transport.commands == []


def test_path_resolution_stays_fixed_and_hash_is_not_repeated(tmp_path, monkeypatch):
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    binary = fake(first, "rclone")
    fake(second, "rclone")
    expected = sha(binary)
    monkeypatch.setenv("PATH", str(first))
    transport = RcloneTransport(expected_sha256=expected)
    identity = transport.binary_identity()
    original = Path.open

    def refuse_rehash(path, *args, **kwargs):
        if path == binary:
            raise AssertionError("binary was re-read after its initial hash")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", refuse_rehash)
    monkeypatch.setenv("PATH", str(second))
    assert transport.version() == "rclone fixture"
    assert transport.provenance()["binary"] == identity
    assert identity["pin_verified"] and identity["sha256"] == expected
    assert all(command[0] == str(binary) for command in transport.commands)
    assert not (second / "executed").exists()


def test_executable_replacement_is_refused_before_execution(tmp_path):
    binary = fake(tmp_path)
    transport = RcloneTransport(binary, expected_sha256=sha(binary))
    transport.binary_identity()
    replacement = tmp_path / "replacement"
    replacement.write_bytes(binary.read_bytes())
    replacement.chmod(0o755)
    replacement.replace(binary)
    with pytest.raises(ValueError, match="changed after verification"):
        transport.version()
    assert not (tmp_path / "executed").exists()


def test_unpinned_provenance_is_explicit(tmp_path):
    binary = fake(tmp_path)
    value = RcloneTransport(binary).provenance()
    assert value["binary"]["sha256"] == sha(binary)
    assert value["binary"]["expected_sha256"] is None
    assert value["binary"]["pin_verified"] is False
    assert value["version"] == "rclone fixture"


def test_cli_rejects_invalid_pin_before_configuration(tmp_path):
    with pytest.raises(SystemExit) as failure:
        main(
            [
                "push",
                str(tmp_path / "source"),
                "0" * 64,
                "--remote",
                "missing",
                "--remotes",
                str(tmp_path / "missing"),
                "--evidence",
                str(tmp_path / "evidence"),
                "--rclone-sha256",
                "bad",
            ]
        )
    assert failure.value.code == 2
    assert not list(tmp_path.iterdir())


def config(profile, endpoint="http://127.0.0.1:13900"):
    return dict(
        endpoint=endpoint,
        bucket="pinning-test",
        prefix="retained",
        region="garage",
        profile="ozx",
        rclone_remote="payload",
        rclone_config=str(profile),
        addressing_style="path",
        format=REPOSITORY["format"],
    )


def test_connection_mismatch_precedes_sdk_credentials(tmp_path, monkeypatch):
    binary = fake(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("SDK credentials were accessed before pin rejection")

    monkeypatch.setattr(boto3, "Session", forbidden)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        connection(
            config(tmp_path / "absent.conf"),
            binary=str(binary),
            expected_sha256="0" * 64,
        )
    assert not (tmp_path / "executed").exists()


def test_pinned_push_and_prepare_receipts(tmp_path, local_s3):
    endpoint, profile = local_s3
    binary = Path(shutil.which("rclone")).resolve()
    expected = sha(binary)
    api = boto3.Session(profile_name="ozx").client(
        "s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"})
    )
    api.create_bucket(Bucket="pinning-test")
    aliases = tmp_path / "aliases.json"
    aliases.write_bytes(canonical({"fixture": config(profile, endpoint)}))
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"payload bytes")
    store = Store(tmp_path / "publisher")
    rid = store.publish(
        source,
        {"version": 2, "partitions": 4, "files": ["data"], "grids": []},
        source_id="pin-test",
        assume_immutable=True,
    )["release_id"]
    evidence = tmp_path / "push"
    assert (
        main(
            [
                "push",
                str(store.path),
                rid,
                "--remote",
                "fixture",
                "--remotes",
                str(aliases),
                "--evidence",
                str(evidence),
                "--rclone-binary",
                str(binary),
                "--rclone-sha256",
                expected,
            ]
        )
        == 0
    )
    transport = json.loads((evidence / "push.receipt.json").read_bytes())["transport"]
    assert transport["binary"]["sha256"] == expected
    assert transport["binary"]["pin_verified"] is True
    assert transport["binary"]["path"] == str(binary)
    assert transport["version"].startswith("rclone ")
    mapping = tmp_path / "mapping.json"
    uri = "s3://pinning-test/retained"
    mapping.write_bytes(canonical({uri: "fixture"}))
    reference = {
        "version": 1,
        "release_id": rid,
        "locations": [{"storage_platform": "s3", "location_uri": uri}],
    }
    reference_path = tmp_path / "reference.json"
    reference_path.write_bytes(canonical(reference))
    assert (
        prepare_main(
            [
                str(reference_path),
                str(tmp_path / "cache"),
                str(tmp_path / "checkout"),
                "--evidence",
                str(tmp_path / "prepare"),
                "--remotes",
                str(aliases),
                "--location-remotes",
                str(mapping),
                "--rclone-binary",
                str(binary),
                "--rclone-sha256",
                expected,
            ]
        )
        == 0
    )
    receipt = json.loads((tmp_path / "prepare/prepare.receipt.json").read_bytes())
    assert receipt["fetch"]["transport"] == transport
    assert (tmp_path / "checkout/data").read_bytes() == b"payload bytes"
    with pytest.raises(ValueError, match="no candidate"):
        prepare(
            reference,
            tmp_path / "bad-cache",
            tmp_path / "bad-checkout",
            tmp_path / "bad",
            remotes=aliases,
            location_remotes=mapping,
            binary=str(binary),
            expected_sha256="0" * 64,
        )
    assert not (tmp_path / "bad-checkout").exists()
    assert not (tmp_path / "bad/prepare.receipt.json").exists()
    resolution = json.loads((tmp_path / "bad/resolution.json").read_bytes())
    assert "SHA-256 mismatch" in resolution["attempts"][0]["reason"]
