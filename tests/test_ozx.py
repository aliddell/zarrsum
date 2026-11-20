"""Exercise repeatable OZX output and the optional loopback S3 demo."""

import json
import os
import socket
import struct
import subprocess
import time
from collections import Counter
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from zipfile import ZIP_STORED, ZipFile

import boto3
import pytest
from botocore.config import Config

from zarrsum.graph_index import Workspace
from zarrsum.ozx import write_ozx
from zarrsum.releases import Store, canonical, digest, main
from zarrsum.remotes import REPOSITORY, graph, local_get


@pytest.fixture
def release(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    side = 32
    pixels = {
        f"{row}.{column}": struct.pack("<H", (row * side + column) % 256)
        for row in range(side)
        for column in range(side)
    }
    members = {
        **pixels,
        ".zarray": canonical(
            {
                "zarr_format": 2,
                "shape": [side, side],
                "chunks": [1, 1],
                "dtype": "<u2",
                "compressor": None,
                "filters": None,
                "fill_value": 0,
                "order": "C",
            }
        ),
        ".zattrs": b'{"dataset":"synthetic OZX demo"}',
    }
    for name, data in members.items():
        (source / name).write_bytes(data)
    store = Store(tmp_path / "publisher")
    release_id = store.publish(
        source,
        {"version": 2, "files": list(members), "grids": [], "partitions": 8},
        source_id="synthetic-demo",
        assume_immutable=True,
    )["release_id"]
    return store, release_id, members


def assert_archive(path, members):
    assert path.stat().st_mode & 0o222 == 0
    with ZipFile(path) as archive:
        assert len(archive.infolist()) == len(members)
        assert set(archive.namelist()) == members.keys()
        for info in archive.infolist():
            assert info.compress_type == ZIP_STORED
            assert archive.read(info) == members[info.filename]


def test_pinned_ozx_reuses_payloads_and_preserves_existing_output(tmp_path, release):
    store, release_id, members = release
    calls = Counter()

    def get(key):
        calls[key] += 1
        return local_get(store.path, key)

    one = tmp_path / "first.ozx"
    result = write_ozx(release_id, get, one, workers=4)
    assert_archive(one, members)
    assert result["downloaded_objects"] == len(set(members.values()))
    assert all(count == 1 for count in calls.values())
    two = tmp_path / "second.ozx"
    repeat = write_ozx(release_id, get, two, workers=1)
    assert (
        repeat["archive_blake3"] == result["archive_blake3"] == digest(one.read_bytes())
    )
    assert one.read_bytes() == two.read_bytes()
    with pytest.raises(FileExistsError):
        write_ozx(release_id, get, one)
    assert_archive(one, members)


@pytest.mark.parametrize("failure", ["corrupt", "missing", "interrupt"])
def test_failed_download_leaves_no_archive(tmp_path, release, failure):
    store, release_id, _ = release

    def get(key):
        data = local_get(store.path, key)
        if key.startswith("objects/") and key.split("/")[1] >= "f0":
            if failure == "corrupt":
                return bytes([data[0] ^ 1]) + data[1:]
            if failure == "missing":
                raise FileNotFoundError(key)
            raise KeyboardInterrupt
        return data

    target = tmp_path / "failed.ozx"
    error = {
        "corrupt": ValueError,
        "missing": FileNotFoundError,
        "interrupt": KeyboardInterrupt,
    }[failure]
    with pytest.raises(error):
        write_ozx(release_id, get, target)
    assert not target.exists()
    assert not list(tmp_path.glob(".ozx-*"))


@pytest.fixture
def local_s3(tmp_path, monkeypatch):
    binary = os.environ.get("OZX_MINIO_BINARY")
    if not binary:
        pytest.skip("set OZX_MINIO_BINARY to run the loopback S3 demo")
    binary = str(Path(binary).resolve())
    user = "ozx-fixture"
    password = "ozx-fixture-only"
    credentials = tmp_path / "credentials"
    config = tmp_path / "aws-config"
    credentials.write_text(
        f"[ozx]\naws_access_key_id={user}\naws_secret_access_key={password}\n"
    )
    config.write_text("[profile ozx]\nregion=garage\n")
    for name, value in {
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials),
        "AWS_CONFIG_FILE": str(config),
        "AWS_EC2_METADATA_DISABLED": "true",
    }.items():
        monkeypatch.setenv(name, value)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "MINIO_ROOT_USER": user,
        "MINIO_ROOT_PASSWORD": password,
        "MINIO_REGION_NAME": "garage",
        "MINIO_BROWSER": "off",
    }
    with (tmp_path / "minio.log").open("wb") as log:
        process = subprocess.Popen(
            [
                binary,
                "--certs-dir",
                str(tmp_path / "certs"),
                "server",
                "--address",
                f"127.0.0.1:{port}",
                "--console-address",
                "127.0.0.1:0",
                str(tmp_path / "minio"),
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            for _ in range(100):
                assert process.poll() is None, (tmp_path / "minio.log").read_text()
                try:
                    with urlopen(endpoint + "/minio/health/ready", timeout=1):
                        break
                except URLError:
                    time.sleep(0.1)
            else:
                pytest.fail("loopback MinIO did not become ready")
            profile = tmp_path / "rclone.conf"
            profile.write_text(
                f"[payload]\ntype=s3\nprovider=Minio\nendpoint={endpoint}\n"
                "region=garage\nenv_auth=false\nforce_path_style=true\n"
                f"access_key_id={user}\nsecret_access_key={password}\n"
            )
            yield endpoint, profile
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.mark.parametrize(
    ("positionals", "message"),
    [
        (
            ["store", "release", "destination"],
            "expected a 64-character lowercase digest",
        ),
        (
            ["store", "--rclone-config", "config", "release", "destination"],
            "expected a 64-character lowercase digest",
        ),
        (["release", "destination"], "pull requires STORE"),
        (["--ozx", "store", "release", "destination"], "without STORE"),
    ],
)
def test_pull_positionals_select_output_mode(tmp_path, capsys, positionals, message):
    with pytest.raises(SystemExit) as refusal:
        main(
            [
                "pull",
                *positionals,
                "--evidence",
                str(tmp_path / "evidence"),
                "--remote",
                "demo",
                "--remotes",
                str(tmp_path / "remotes.json"),
            ]
        )
    assert refusal.value.code == 2
    assert message in capsys.readouterr().err


def test_pull_ozx_from_loopback_s3(tmp_path, release, local_s3):
    store, release_id, members = release
    endpoint, profile = local_s3
    client = boto3.Session(profile_name="ozx").client(
        "s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"})
    )
    bucket = "ozx-fixture"
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Bucket=bucket, Key="release/repository.json", Body=canonical(REPOSITORY)
    )
    workspace = Workspace()
    try:
        _, content, _ = graph(
            release_id, lambda key: local_get(store.path, key), workspace=workspace
        )
        graph_keys = len(content)
        for key in content:
            client.put_object(
                Bucket=bucket, Key=f"release/{key}", Body=local_get(store.path, key)
            )
    finally:
        workspace.close()

    aliases = tmp_path / "remotes.json"
    aliases.write_bytes(
        canonical(
            {
                "demo": {
                    "endpoint": endpoint,
                    "bucket": bucket,
                    "prefix": "release",
                    "region": "garage",
                    "addressing_style": "path",
                    "profile": "ozx",
                    "rclone_remote": "payload",
                    "rclone_config": str(profile),
                    "format": REPOSITORY["format"],
                }
            }
        )
    )
    output = tmp_path / "demo.ozx"
    original = tmp_path / "source"
    original.rename(tmp_path / "unavailable-source")
    store.path.rename(tmp_path / "unavailable-publisher")
    evidence = tmp_path / "evidence"
    args = [
        "pull",
        "--ozx",
        "--rclone-binary", "must-not-execute-for-sdk-output",
        release_id,
        str(output),
        "--remote",
        "demo",
        "--remotes",
        str(aliases),
        "--evidence",
        str(evidence),
    ]
    assert main(args) == 0
    assert_archive(output, members)
    receipt = json.loads((evidence / "pull-ozx.receipt.json").read_bytes())
    assert receipt["release_id"] == release_id
    assert "transport" not in receipt
    assert receipt["files"] == len(members)
    assert receipt["downloaded_objects"] == len(set(members.values()))
    assert receipt["archive_blake3"] == digest(output.read_bytes())
    evidence_json = json.loads((evidence / "result.json").read_bytes())
    assert evidence_json["sdk_requests"] == {"GetObject": graph_keys + 1}
    print(f"Verified {receipt['files']} archive members at {output}")

    directory = tmp_path / "directory"
    native_evidence = tmp_path / "native-evidence"
    assert main([
        "pull", str(tmp_path / "cache"), release_id, str(directory),
        "--remote", "demo", "--remotes", str(aliases),
        "--evidence", str(native_evidence), "--transfer-batch-size", "1024",
    ]) == 0
    for name, expected in members.items():
        assert (directory / name).read_bytes() == expected
    native_result = json.loads((native_evidence / "result.json").read_bytes())
    assert native_result["transfer_batch_size"] == 1024
    assert native_result["result"] == "verified"


def test_zip64_member_count_boundary(tmp_path, monkeypatch):
    import zarrsum.ozx as ozx

    payload = b"x"
    checksum = digest(payload)
    records = {
        f"array/{index:05d}": (f"array/{index:05d}", len(payload), checksum)
        for index in range(65_536)
    }
    monkeypatch.setattr(ozx, "graph", lambda *args, **kwargs: (
        None, {"objects/" + checksum: (len(payload), checksum)}, records,
    ))
    target = tmp_path / "zip64.ozx"
    receipt = write_ozx("0" * 64, lambda key: payload, target)
    assert receipt["files"] == 65_536 and receipt["downloaded_objects"] == 1
    with ZipFile(target) as archive:
        assert len(archive.infolist()) == 65_536
        assert archive.testzip() is None
    assert b"PK\x06\x06" in target.read_bytes()
