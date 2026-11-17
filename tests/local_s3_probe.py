"""Probe the local S3 contract before release transfer."""

import concurrent.futures
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path("/mnt/main0/home/aliddell/scratch/local-s3-20261117")


def fixture(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    config = directory / "garage.toml"
    config.write_text(f'''metadata_dir = "{directory}/meta"
data_dir = "{directory}/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "127.0.0.1:13901"
rpc_public_addr = "127.0.0.1:13901"
rpc_secret = "{secrets.token_hex(32)}"
[s3_api]
s3_region = "garage"
api_bind_addr = "127.0.0.1:13900"
''')
    config.chmod(0o600)
    command = [str(ROOT / "garage"), "-c", str(config)]
    log = open(directory / "server.log", "w")
    server = subprocess.Popen(command + ["server"], stdout=log, stderr=log)

    def cli(*args):
        p = subprocess.run(
            command + list(args), capture_output=True, text=True, timeout=30
        )
        if p.returncode:
            raise RuntimeError(
                "Garage CLI failed: " + args[0] + ": " + p.stderr[-1000:]
            )
        return p.stdout

    for _ in range(100):
        p = subprocess.run(
            command + ["node", "id", "--quiet"], capture_output=True, text=True
        )
        if p.returncode == 0:
            break
        time.sleep(0.1)
    else:
        server.terminate()
        raise RuntimeError("Garage did not start")
    node = p.stdout.strip().split("@")[0]
    cli("layout", "assign", "-z", "test", "-c", "1G", node)
    cli("layout", "apply", "--version", "1")
    key_id, secret = "GK" + secrets.token_hex(12), secrets.token_hex(32)
    cli("key", "import", "--yes", "-n", "local-test", key_id, secret)
    cli("bucket", "create", "release-test")
    cli(
        "bucket",
        "allow",
        "--read",
        "--write",
        "--owner",
        "release-test",
        "--key",
        key_id,
    )
    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:13900",
        region_name="garage",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    rconfig = directory / "rclone.conf"
    rconfig.write_text(
        f"[s3]\ntype = s3\nprovider = Other\nenv_auth = false\naccess_key_id = {key_id}\nsecret_access_key = {secret}\nendpoint = http://127.0.0.1:13900\nregion = garage\nforce_path_style = true\nno_check_bucket = true\n"
    )
    rconfig.chmod(0o600)
    return client, server, command, rconfig, log


def minio_fixture(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    key_id, secret = secrets.token_hex(10), secrets.token_hex(24)
    env = {
        **os.environ,
        "MINIO_ROOT_USER": key_id,
        "MINIO_ROOT_PASSWORD": secret,
        "MINIO_BROWSER": "off",
        "MINIO_UPDATE": "off",
    }
    command = [
        str(ROOT / "minio"),
        "server",
        "--address",
        "127.0.0.1:13900",
        "--console-address",
        "127.0.0.1:13902",
        str(directory / "data"),
    ]
    log = open(directory / "server.log", "w")
    server = subprocess.Popen(command, env=env, stdout=log, stderr=log)
    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:13900",
        region_name="garage",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
            connect_timeout=2,
            read_timeout=5,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    for _ in range(100):
        try:
            client.list_buckets()
            break
        except Exception:
            if server.poll() is not None:
                raise RuntimeError("MinIO failed to start")
            time.sleep(0.1)
    else:
        server.terminate()
        raise RuntimeError("MinIO did not start")
    client.create_bucket(Bucket="release-test")
    rconfig = directory / "rclone.conf"
    rconfig.write_text(
        f"[s3]\ntype = s3\nprovider = Minio\nenv_auth = false\naccess_key_id = {key_id}\nsecret_access_key = {secret}\nendpoint = http://127.0.0.1:13900\nregion = garage\nforce_path_style = true\nno_check_bucket = true\n"
    )
    rconfig.chmod(0o600)
    return client, server, (command, env), rconfig, log


def probe(client, rconfig):
    bucket = "release-test"
    result = {}

    def put(key, body, **kw):
        return client.put_object(Bucket=bucket, Key=key, Body=body, **kw)

    key = "probe/exact space+percent%/x"
    put(key, b"correct")
    assert client.head_object(Bucket=bucket, Key=key)["ContentLength"] == 7
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"correct"
    result["put_get_head_exact_key"] = "pass"
    try:
        client.head_object(Bucket=bucket, Key="absent")
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        result["missing_head"] = 404
    upload = client.create_multipart_upload(Bucket=bucket, Key="probe/multipart")[
        "UploadId"
    ]
    parts = []
    for number, body in enumerate([b"a" * (5 << 20), b"b" * 123], 1):
        etag = client.upload_part(
            Bucket=bucket,
            Key="probe/multipart",
            UploadId=upload,
            PartNumber=number,
            Body=body,
        )["ETag"]
        parts.append({"PartNumber": number, "ETag": etag})
    client.complete_multipart_upload(
        Bucket=bucket,
        Key="probe/multipart",
        UploadId=upload,
        MultipartUpload={"Parts": parts},
    )
    assert (
        client.get_object(Bucket=bucket, Key="probe/multipart")["Body"].read()
        == b"a" * (5 << 20) + b"b" * 123
    )
    result["multipart_bytes"] = (5 << 20) + 123

    def race(body):
        try:
            response = put("probe/conditional", body, IfNoneMatch="*")
            return [response["ResponseMetadata"]["HTTPStatusCode"], body.decode()]
        except ClientError as e:
            return [e.response["ResponseMetadata"]["HTTPStatusCode"], body.decode()]

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        outcomes = list(pool.map(race, [b"writer-one", b"writer-two"]))
    result["conditional_race"] = outcomes
    winner = [body for status, body in outcomes if status == 200]
    result["conditional_pass"] = sorted(x[0] for x in outcomes) == [200, 412]
    stored = (
        client.get_object(Bucket=bucket, Key="probe/conditional")["Body"]
        .read()
        .decode()
    )
    result["conditional_stored_matches_winner"] = (
        len(winner) == 1 and stored == winner[0]
    )
    rclone = ["/mnt/main0/home/aliddell/.bin/rclone", "--config", str(rconfig)]
    out = subprocess.run(
        rclone + ["cat", "s3:release-test/" + key], capture_output=True, timeout=30
    )
    assert out.returncode == 0 and out.stdout == b"correct"
    result["rclone_get"] = "pass"
    return result


if __name__ == "__main__":
    for key in list(os.environ):
        if key.startswith(("AWS_", "RCLONE_", "HERMES_")):
            del os.environ[key]
    os.environ.update(
        AWS_EC2_METADATA_DISABLED="true",
        AWS_CONFIG_FILE="/dev/null",
        AWS_SHARED_CREDENTIALS_FILE="/dev/null",
    )
    directory = Path(tempfile.mkdtemp(prefix="dv-local-s3-", dir="/tmp"))
    result = {
        "host": os.uname().nodename,
        "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "directory": str(directory),
    }
    client = server = None
    try:
        client, server, command, rconfig, log = (
            minio_fixture if os.environ.get("DV_USE_MINIO") else fixture
        )(directory)
        result.update(probe(client, rconfig))
    finally:
        if server:
            server.terminate()
            server.wait(timeout=30)
        result["end_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        (ROOT / ("probe-" + os.environ["SLURM_JOB_ID"] + ".json")).write_text(
            json.dumps(result, indent=2)
        )
        import shutil

        shutil.copyfile(
            directory / "server.log",
            ROOT / ("server-probe-" + os.environ["SLURM_JOB_ID"] + ".log"),
        )
        shutil.rmtree(directory)
