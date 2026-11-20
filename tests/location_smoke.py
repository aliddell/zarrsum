"""Exercise installed preparation against owned filesystem stores and pinned MinIO.

Run on a compute node. This is a tiny correctness test, not a throughput benchmark.
"""

import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import boto3
from botocore.config import Config

from zarrsum.releases import Store, canonical
from zarrsum.remotes import graph, local_get


def main():
    root, code = map(Path, sys.argv[1:3])
    job = os.environ["SLURM_JOB_ID"]
    out = root / ("run-" + job)
    out.mkdir()
    shared = root / ("shared-" + job)
    shared.mkdir()
    work = Path(tempfile.mkdtemp(prefix="dv-location-smoke-", dir="/tmp"))
    result = {
        "job": job,
        "host": socket.gethostname(),
        "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_commit": subprocess.check_output(
            ["git", "-C", str(code), "rev-parse", "HEAD"], text=True
        ).strip(),
        "source_sha256": {
            str(p.relative_to(code)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (code / "src/zarrsum").glob("*.py")
        },
        "test": "single-host tiny correctness test; no performance comparison",
    }
    commands = []
    server = None
    access_key, secret_key = secrets.token_hex(10), secrets.token_hex(24)
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("AWS_", "RCLONE_", "HERMES_"))
    }
    environment.update(
        AWS_EC2_METADATA_DISABLED="true",
        AWS_CONFIG_FILE="/dev/null",
        AWS_SHARED_CREDENTIALS_FILE="/dev/null",
        PYTHONDONTWRITEBYTECODE="1",
    )

    def cli(args, expected=0):
        args = [str(code / ".venv/bin/zarrsum-release"), *map(str, args)]
        process = subprocess.run(
            args, env=environment, capture_output=True, text=True, timeout=90
        )
        commands.append(
            {
                "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "argv": args,
                "exit": process.returncode,
                "stdout": process.stdout,
                "stderr": process.stderr,
            }
        )
        assert (
            (process.returncode == 0) if expected == 0 else (process.returncode != 0)
        ), process.stderr
        return json.loads(process.stdout) if expected == 0 else process.stderr

    try:
        assert (
            hashlib.sha256((root / "minio").read_bytes()).hexdigest()
            == "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f"
        )
        assert shutil.disk_usage(work).free > 1 << 30
        source = shared / "source"
        source.mkdir()
        files = ["image-a", "image-b", "metadata.json"]
        for number, name in enumerate(files):
            (source / name).write_bytes((str(number) + name).encode())
        store = Store(shared / "original")
        rid = store.publish(
            source,
            {"version": 2, "partitions": 4, "files": files, "grids": []},
            source_id="location-smoke",
            assume_immutable=True,
        )["release_id"]
        ref = shared / "release.json"
        ref.write_bytes(
            canonical(
                {
                    "version": 1,
                    "release_id": rid,
                    "locations": [
                        {
                            "storage_platform": "reef",
                            "location_uri": "reef://" + str(store.path),
                        }
                    ],
                }
            )
        )
        first = cli(
            [
                "prepare",
                ref,
                work / "local-cache",
                work / "first-checkout",
                "--local-platform",
                "reef",
                "--evidence",
                out / "filesystem",
            ]
        )
        assert first["release_id"] == rid
        replica = shared / "replica"
        shutil.copytree(work / "local-cache", replica)
        shutil.rmtree(store.path)
        shutil.rmtree(source)
        ref.write_bytes(
            canonical(
                {
                    "version": 1,
                    "release_id": rid,
                    "locations": [
                        {
                            "storage_platform": "reef",
                            "location_uri": "reef://" + str(replica),
                        }
                    ],
                }
            )
        )
        relocated = cli(
            [
                "prepare",
                ref,
                work / "relocated-cache",
                work / "relocated-checkout",
                "--local-platform",
                "reef",
                "--evidence",
                out / "relocated",
            ]
        )
        assert relocated["release_id"] == rid
        log = open(work / "minio.log", "w")
        server = subprocess.Popen(
            [
                str(root / "minio"),
                "server",
                "--address",
                "127.0.0.1:13900",
                "--console-address",
                "127.0.0.1:13902",
                str(work / "server"),
            ],
            env={
                **environment,
                "MINIO_ROOT_USER": access_key,
                "MINIO_ROOT_PASSWORD": secret_key,
                "MINIO_BROWSER": "off",
                "MINIO_UPDATE": "off",
            },
            stdout=log,
            stderr=log,
        )
        client = boto3.client(
            "s3",
            endpoint_url="http://127.0.0.1:13900",
            region_name="garage",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
        )
        for _ in range(100):
            try:
                client.list_buckets()
                break
            except Exception:
                assert server.poll() is None
                time.sleep(0.1)
        else:
            raise TimeoutError("MinIO did not start")
        client.create_bucket(Bucket="release-test")
        credentials = work / "credentials"
        credentials.write_text(
            "[fixture]\naws_access_key_id="
            + access_key
            + "\naws_secret_access_key="
            + secret_key
            + "\n"
        )
        credentials.chmod(0o600)
        environment["AWS_SHARED_CREDENTIALS_FILE"] = str(credentials)
        rconfig = work / "rclone.conf"
        rconfig.write_text(
            "[s3]\ntype=s3\nprovider=Minio\nenv_auth=false\naccess_key_id="
            + access_key
            + "\nsecret_access_key="
            + secret_key
            + "\nendpoint=http://127.0.0.1:13900\nregion=garage\nforce_path_style=true\nno_check_bucket=true\n"
        )
        rconfig.chmod(0o600)
        remotes = work / "remotes.json"
        cli(
            [
                "remote",
                "--config",
                remotes,
                "add",
                "publisher",
                "--endpoint",
                "http://127.0.0.1:13900",
                "--bucket",
                "release-test",
                "--prefix",
                "retained",
                "--region",
                "garage",
                "--profile",
                "fixture",
                "--rclone-remote",
                "s3",
                "--rclone-config",
                rconfig,
            ]
        )
        pushed = cli(
            [
                "push",
                replica,
                rid,
                "--remote",
                "publisher",
                "--remotes",
                remotes,
                "--evidence",
                out / "push",
                "--rclone-binary",
                "/mnt/main0/home/aliddell/.bin/rclone",
                "--transfer-timeout",
                "35",
            ]
        )
        assert pushed["result"] == "verified"
        _, content, _ = graph(rid, lambda key: local_get(replica, key))
        payload_key = next(key for key in content if key.startswith("objects/"))
        good = local_get(replica, payload_key)
        shutil.rmtree(replica)
        shutil.rmtree(work / "local-cache")
        shutil.rmtree(work / "relocated-cache")
        uri = "s3://release-test/retained"
        ref.write_bytes(
            canonical(
                {
                    "version": 1,
                    "release_id": rid,
                    "locations": [
                        {
                            "storage_platform": "reef",
                            "location_uri": "reef://" + str(shared / "missing"),
                        },
                        {"storage_platform": "s3", "location_uri": uri},
                    ],
                }
            )
        )
        access = work / "location-remotes.json"
        access.write_bytes(canonical({uri: "publisher"}))
        args = [
            "--local-platform",
            "reef",
            "--remotes",
            remotes,
            "--location-remotes",
            access,
            "--rclone-binary",
            "/mnt/main0/home/aliddell/.bin/rclone",
            "--transfer-timeout",
            "35",
        ]
        client.put_object(
            Bucket="release-test",
            Key="retained/" + payload_key,
            Body=bytes([good[0] ^ 1]) + good[1:],
        )
        cli(
            [
                "prepare",
                ref,
                work / "bad-cache",
                work / "bad-checkout",
                "--evidence",
                out / "corrupt",
                *args,
            ],
            expected=2,
        )
        assert not (work / "bad-checkout").exists()
        assert not (work / "bad-cache/releases" / rid).exists()
        assert (out / "corrupt/attempt-0001/transport.json").exists()
        client.put_object(
            Bucket="release-test", Key="retained/" + payload_key, Body=good
        )
        fetched = cli(
            [
                "prepare",
                ref,
                work / "s3-cache",
                work / "s3-checkout",
                "--evidence",
                out / "s3",
                *args,
            ]
        )
        receipt = json.loads(Path(fetched["receipt"]).read_bytes())
        assert receipt["selected_location"]["storage_platform"] == "s3"
        assert [item["result"] for item in receipt["attempts"]] == [
            "refused",
            "verified",
        ]
        assert receipt["checkout"]["files"] == 3
        assert receipt["fetch"]["downloaded_objects"] == 3
        server.terminate()
        server.wait(timeout=20)
        server = None
        log.close()
        try:
            with socket.create_connection(("127.0.0.1", 13900), timeout=1):
                pass
        except OSError:
            result["server_stopped"] = True
        else:
            raise AssertionError("server still listening")
        for path in [credentials, rconfig, remotes, access]:
            path.unlink()
        offline = cli(
            [
                "prepare",
                ref,
                work / "s3-cache",
                work / "offline-checkout",
                "--evidence",
                out / "offline",
                *args,
            ]
        )
        offline_receipt = json.loads(Path(offline["receipt"]).read_bytes())
        assert offline_receipt["cache_hit"] and not offline_receipt["attempts"]
        existing = work / "existing"
        existing.mkdir()
        (existing / "sentinel").write_bytes(b"keep")
        cli(
            [
                "prepare",
                ref,
                work / "unused-cache",
                existing,
                "--evidence",
                out / "existing",
                *args,
            ],
            expected=2,
        )
        assert (existing / "sentinel").read_bytes() == b"keep"
        assert not (work / "unused-cache").exists()
        result["pass"] = True
        result.update(
            release_id=rid,
            files=3,
            content_keys=len(content),
            relocation_same_id=True,
            original_source_and_store_absent=True,
            s3_corruption_refused=True,
            fallback_pass=True,
            offline_pass=True,
            existing_destination_preserved=True,
            minio_version=subprocess.check_output(
                [str(root / "minio"), "--version"], text=True
            ),
            rclone_version=subprocess.check_output(
                ["/mnt/main0/home/aliddell/.bin/rclone", "version"], text=True
            ),
        )
    finally:
        if server is not None:
            server.terminate()
            server.wait(timeout=20)
        shutil.rmtree(work)
        shutil.rmtree(shared)
        result["owned_scratch_removed"] = not work.exists() and not shared.exists()
        result["end_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for value in commands:
            for key in ["stdout", "stderr"]:
                value[key] = (
                    value[key]
                    .replace(access_key, "<redacted>")
                    .replace(secret_key, "<redacted>")
                )
        (out / "commands.json").write_text(json.dumps(commands, indent=2))
        (out / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
