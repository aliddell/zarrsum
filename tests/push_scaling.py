"""Exercise a pinned release through S3 between two exclusive compute nodes.

Shared storage carries code, credentials, control messages, and evidence. All payload
stores and checkouts use each node's private /tmp directory.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import psutil

from zarrsum.releases import Store, canonical, digest, keys, atomic_file
from zarrsum.remotes import graph, local_get

A = "729ce82fb94fc7eb9b4bea7ef688dd1374015d672d2c3294fe4deb2a14b5ee34"
ORIGINAL = Path("/mnt/main0/home/aliddell/scratch/representative-20261117/store")
ROOT = Path(sys.argv[1])
CONTROL = Path(sys.argv[2])
RANK = int(os.environ["SLURM_PROCID"])
HOST = socket.gethostname()
OUT = CONTROL.parent / ("node-" + str(RANK))
OUT.mkdir(parents=True, exist_ok=True)
RESULT = {"host": HOST, "rank": RANK, "start_utc": "", "timings": [], "checks": {}}
BOUNDARIES = []
WORK = None
SERVER = None
SECRET_VALUES = []


def utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def save():
    (OUT / "result.json").write_text(json.dumps(RESULT, indent=2, default=str))


def timed(name, action):
    start = time.monotonic()
    value = action()
    RESULT["timings"].append(
        {"operation": name, "wall_s": time.monotonic() - start, "utc": utc()}
    )
    save()
    return value


def wait_file(path, seconds=180):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if path.exists():
            return json.loads(path.read_bytes())
        if (CONTROL / "abort.json").exists():
            raise RuntimeError(
                "peer aborted: "
                + str(json.loads((CONTROL / "abort.json").read_bytes()))
            )
        time.sleep(0.05)
    raise TimeoutError("control file timeout: " + str(path))


def failure(name, action, absent=None):
    try:
        action()
    except (ValueError, FileNotFoundError, FileExistsError) as error:
        if absent is not None:
            assert not absent.exists()
        RESULT["checks"][name] = {
            "pass": True,
            "exception": type(error).__name__,
            "detail": str(error)[:600],
        }
        save()
        return
    raise AssertionError("negative check succeeded: " + name)


def client_for(session):
    return boto3.client(
        "s3",
        endpoint_url=session["endpoint"],
        region_name="garage",
        aws_access_key_id=session["access"],
        aws_secret_access_key=session["secret"],
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
            connect_timeout=5,
            read_timeout=30,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def rconfig_for(session):
    path = WORK / "rclone.conf"
    path.write_text(
        "[s3]\ntype = s3\nprovider = Minio\nenv_auth = false\n"
        "access_key_id = "
        + session["access"]
        + "\nsecret_access_key = "
        + session["secret"]
        + "\n"
        "endpoint = "
        + session["endpoint"]
        + "\nregion = garage\nforce_path_style = true\nno_check_bucket = true\n"
    )
    path.chmod(0o600)
    credentials = WORK / "sdk-credentials"
    credentials.write_text(
        "[fixture]\naws_access_key_id="
        + session["access"]
        + "\naws_secret_access_key="
        + session["secret"]
        + "\n"
    )
    credentials.chmod(0o600)
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(credentials)
    os.environ["AWS_CONFIG_FILE"] = "/dev/null"
    return path


class CLIRelease:
    def __init__(self, client, config, name, prefix):
        self.client, self.config, self.name, self.prefix = client, config, name, prefix
        self.events, self.transfers, self.requests = [], [], {}
        self.aliases = WORK / "remotes.json"
        self.add()

    def add(self):
        self.command(
            [
                "remote",
                "--config",
                str(self.aliases),
                "add",
                self.name,
                "--endpoint",
                self.client.meta.endpoint_url,
                "--bucket",
                "release-test",
                "--prefix",
                self.prefix,
                "--region",
                "garage",
                "--profile",
                "fixture",
                "--rclone-remote",
                "s3",
                "--rclone-config",
                str(self.config),
            ]
        )

    def command(self, arguments, evidence=None):
        command = [str(ROOT / "venv/bin/zarrsum-release"), *arguments]
        environment = {
            k: v
            for k, v in os.environ.items()
            if k != "PYTHONPATH" and not k.startswith("HERMES_")
        }
        if os.environ.get("DV_SOURCE_DENY_JSON"):
            environment["PYTHONPATH"] = str(WORK / "deny-source")
        process = subprocess.run(
            command, env=environment, capture_output=True, text=True
        )
        if evidence is not None and (evidence / "result.json").exists():
            value = json.loads((evidence / "result.json").read_bytes())
            self.events.extend(value.get("events", []))
            self.transfers.extend(value.get("transfers", []))
            for key, count in value.get("sdk_requests", {}).items():
                self.requests[key] = self.requests.get(key, 0) + count
        if process.returncode:
            raise ValueError(f"exit {process.returncode}: " + process.stderr.strip())
        return json.loads(process.stdout)

    def operation(self, operation, store, rid, name, destination=None):
        evidence = OUT / self.name / name
        arguments = [operation, str(store), rid]
        if destination is not None:
            arguments.append(str(destination))
        arguments += ["--evidence", str(evidence)]
        if operation != "checkout":
            arguments += [
                "--remote",
                self.name,
                "--remotes",
                str(self.aliases),
                "--rclone-binary",
                "/usr/bin/rclone",
            ]
            if os.environ.get("DV_DIRECT_BWLIMIT"):
                arguments += ["--bwlimit", os.environ["DV_DIRECT_BWLIMIT"]]
        return self.command(arguments, evidence)

    def push(self, store, rid, name):
        return self.operation("push", store, rid, name)

    def fetch(self, rid, store, name):
        return self.operation("fetch", store, rid, name)

    def checkout(self, store, rid, destination, name):
        return self.operation("checkout", store, rid, name, destination)

    def pull(self, rid, store, destination, name):
        return self.operation("pull", store, rid, name, destination)

    def get(self, key):
        return self.client.get_object(
            Bucket="release-test", Key=self.prefix + "/" + key
        )["Body"].read()

    def put(self, key, data):
        return self.client.put_object(
            Bucket="release-test", Key=self.prefix + "/" + key, Body=data
        )


def boundary(client, config, name, prefix="retained"):
    item = CLIRelease(client, config, name, prefix)
    BOUNDARIES.append((name, item))
    return item


def preserve_graph(store, rid):
    _, contents, records = graph(rid, lambda key: local_get(store, key))
    for key in contents:
        if not key.startswith("objects/"):
            path = OUT / "graphs" / rid / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(local_get(store, key))
    return {
        "graph_keys": len(contents),
        "files": len(records),
        "bytes": sum(row[1] for row in records.values()),
        "payload_objects": sum(key.startswith("objects/") for key in contents),
    }


def start_minio(session, log):
    global SERVER
    command = [
        str(ROOT / "minio"),
        "server",
        "--address",
        session["address"] + ":13900",
        "--console-address",
        "127.0.0.1:13902",
        str(WORK / "service-data"),
    ]
    env = {
        **os.environ,
        "MINIO_ROOT_USER": session["access"],
        "MINIO_ROOT_PASSWORD": session["secret"],
        "MINIO_BROWSER": "off",
        "MINIO_UPDATE": "off",
    }
    SERVER = subprocess.Popen(command, env=env, stdout=log, stderr=log)
    client = client_for(session)
    for _ in range(100):
        try:
            client.list_buckets()
            return client
        except Exception:
            if SERVER.poll() is not None:
                raise RuntimeError("MinIO exited during start")
            time.sleep(0.1)
    raise TimeoutError("MinIO start timeout")


def stop_minio():
    global SERVER
    if SERVER is not None:
        SERVER.terminate()
        SERVER.wait(timeout=20)
        SERVER = None


class Audit:
    """Aggregate S3 audit events in memory. Never retain raw headers or credentials."""

    def __init__(self):
        self.counts = Counter()
        self.clients = Counter()
        self.seen = set()
        self.duplicates = 0
        self.errors = 0
        self.last = time.monotonic()
        self.lock = threading.Lock()
        self.expected = {}
        self.reads = {}
        self.roots = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if length > 1 << 20:
                        raise ValueError("audit batch too large")
                    raw = self.rfile.read(length)
                    try:
                        events = json.loads(raw)
                    except json.JSONDecodeError:
                        events = [json.loads(line) for line in raw.splitlines() if line]
                    if isinstance(events, dict):
                        events = [events]
                    with owner.lock:
                        for event in events:
                            request = event.get("requestID")
                            api = event.get("api", {})
                            if not request:
                                continue
                            if request in owner.seen:
                                owner.duplicates += 1
                                continue
                            if len(owner.seen) >= 3000000:
                                raise ValueError("audit request budget exceeded")
                            owner.seen.add(request)
                            name = api.get("name", "unknown")
                            status = str(api.get("statusCode", "unknown"))
                            owner.counts[name + "|" + status] += 1
                            user = event.get("userAgent", "").lower()
                            owner.clients[
                                "rclone" if "rclone" in user else "sdk-or-other"
                            ] += 1
                            key = api.get("object", "")
                            stamp = event.get("time", "")
                            for prefix, expected in owner.expected.items():
                                if key.startswith(prefix + "/"):
                                    relative = key[len(prefix) + 1 :]
                                    if (
                                        name == "GetObject"
                                        and status == "200"
                                        and relative in expected
                                    ):
                                        owner.reads.setdefault(prefix, {}).setdefault(
                                            relative, stamp
                                        )
                                    if name == "PutObject" and "/releases/" in key:
                                        owner.roots.append(
                                            {
                                                "prefix": prefix,
                                                "key": relative,
                                                "status": status,
                                                "server_time": stamp,
                                            }
                                        )
                            owner.last = time.monotonic()
                    self.send_response(200)
                    self.end_headers()
                except Exception:
                    owner.errors += 1
                    self.send_response(500)
                    self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 13905), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def snapshot(self):
        # The queue is asynchronous. Wait for a quiet interval before phase counters.
        time.sleep(0.5)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and time.monotonic() - self.last < 0.35:
            time.sleep(0.05)
        with self.lock:
            roots = []
            for root in self.roots:
                reads = self.reads.get(root["prefix"], {})
                roots.append(
                    {
                        **root,
                        "expected_keys": len(self.expected[root["prefix"]]),
                        "read_keys_before_root_server_time": sum(
                            stamp <= root["server_time"] for stamp in reads.values()
                        ),
                    }
                )
            return {
                "counts": dict(self.counts),
                "clients": dict(self.clients),
                "events": len(self.seen),
                "duplicates": self.duplicates,
                "collector_errors": self.errors,
                "root_publications": roots,
            }

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


def cli_args(
    remote,
    operation,
    store,
    rid,
    label,
    *,
    destination=None,
    resume=None,
    bandwidth=None,
):
    evidence = OUT / remote.name / label
    args = [str(ROOT / "venv/bin/zarrsum-release"), operation, str(store), rid]
    if destination is not None:
        args.append(str(destination))
    args += ["--evidence", str(evidence)]
    if operation != "checkout":
        args += [
            "--remote",
            remote.name,
            "--remotes",
            str(remote.aliases),
            "--rclone-binary",
            "/usr/bin/rclone",
            "--transfer-timeout",
            "35",
        ]
        if bandwidth:
            args += ["--bwlimit", bandwidth]
        if resume:
            args += ["--resume-plan", str(resume[0]), "--plan-digest", resume[1]]
    return args, evidence


def execute_cli(
    remote,
    operation,
    store,
    rid,
    label,
    *,
    destination=None,
    resume=None,
    bandwidth=None,
    expected=0,
):
    args, evidence = cli_args(
        remote,
        operation,
        store,
        rid,
        label,
        destination=destination,
        resume=resume,
        bandwidth=bandwidth,
    )
    start = time.monotonic()
    process = subprocess.run(args, capture_output=True, text=True, timeout=3600)
    value = (
        json.loads((evidence / "result.json").read_bytes())
        if (evidence / "result.json").exists()
        else {}
    )
    (evidence / "exit.json").write_text(
        json.dumps(
            {
                "argv": args,
                "returncode": process.returncode,
                "wall_s": time.monotonic() - start,
                "stderr": process.stderr[-2000:],
            },
            indent=2,
        )
    )
    if expected == 0:
        assert process.returncode == 0, process.stderr
    else:
        assert process.returncode != 0, "negative CLI attempt succeeded"
    return value


def inventory(client, prefix):
    found = {}
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket="release-test", Prefix=prefix + "/"
    ):
        for item in page.get("Contents", []):
            found[item["Key"][len(prefix) + 1 :]] = item["Size"]
    return found


def root_absent(client, prefix, rid):
    try:
        client.head_object(Bucket="release-test", Key=prefix + "/releases/" + rid)
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        return True
    raise AssertionError("failed push published a release root")


def interrupted_push(client, remote, store, rid, contents, kind, session, log):
    previous_temp = set(WORK.rglob("zarrsum-rclone-*"))
    args, evidence = cli_args(
        remote, "push", store, rid, "interrupted", bandwidth="4M"
    )
    evidence.mkdir(parents=True)
    err = open(evidence / "stderr.txt", "w")
    process = subprocess.Popen(
        args, stdout=subprocess.DEVNULL, stderr=err, start_new_session=True
    )
    complete = []
    children = []
    observed = {}
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline and process.poll() is None:
        observed = inventory(client, remote.prefix)
        complete = []
        for key in observed:
            if key.startswith("objects/") and key in contents:
                data = client.get_object(
                    Bucket="release-test", Key=remote.prefix + "/" + key
                )["Body"].read()
                if len(data) == contents[key][0] and digest(data) == contents[key][1]:
                    complete.append(key)
        children = []
        for child in psutil.Process(process.pid).children(recursive=True):
            try:
                name, cmd = child.name(), child.cmdline()
                if name == "rclone" and "copy" in cmd:
                    children.append(child)
            except psutil.Error:
                pass
        if (
            complete
            and children
            and len([key for key in observed if key in contents]) < len(contents) - 1
        ):
            break
        time.sleep(0.05)
    assert process.poll() is None and complete and children, (
        "no active partial upload observed"
    )
    assert root_absent(client, remote.prefix, rid)
    before = {
        "inventory": observed,
        "complete_payloads": complete,
        "cli_pid": process.pid,
        "children": [{"pid": p.pid, "pgid": os.getpgid(p.pid)} for p in children],
        "required_nonroot_keys": len(contents) - 1,
        "utc": utc(),
    }
    temp_before = set(WORK.rglob("zarrsum-rclone-*")) - previous_temp
    if kind == "kill":
        groups = {os.getpgid(process.pid)} | {os.getpgid(p.pid) for p in children}
        for group in groups:
            os.killpg(group, signal.SIGKILL)
        before["killed_groups"] = sorted(groups)
    elif kind == "server":
        stop_minio()
    else:
        process.send_signal(signal.SIGTERM)
    status = process.wait(timeout=100)
    err.close()
    assert status != 0
    if kind == "kill":
        for child in children:
            try:
                os.waitpid(child.pid, 0)
            except ChildProcessError:
                pass
    deadline = time.monotonic() + 5
    for child in children:
        while time.monotonic() < deadline and psutil.pid_exists(child.pid):
            time.sleep(0.02)
        assert not psutil.pid_exists(child.pid), "rclone child survived interruption"
    if kind == "server":
        start_minio(session, log)
    assert root_absent(client, remote.prefix, rid)
    assert not (evidence / "push.receipt.json").exists()
    remaining = [str(path) for path in temp_before if path.exists()]
    if kind != "kill":
        assert not remaining, "handled failure left transport temporary files"
    plan = evidence / "push.plan.json"
    submission = json.loads((evidence / "push.submission.json").read_bytes())
    assert digest(plan.read_bytes()) == submission["blake3"]
    temp_artifacts = [
        {"path": str(file.relative_to(WORK)), "bytes": file.stat().st_size}
        for directory in remaining
        for file in Path(directory).rglob("*")
        if file.is_file()
    ]
    result = {
        "remaining_temp_artifacts": temp_artifacts,
        **before,
        "exit": status,
        "root_absent_after_failure": True,
        "successful_receipt_absent": True,
        "remaining_owned_temp": remaining,
        "plan_blake3": submission["blake3"],
        "kind": kind,
    }
    for directory in remaining:
        path = Path(directory)
        assert path.is_relative_to(WORK) and not path.is_symlink()
        shutil.rmtree(path)
    result["recorded_temp_artifacts_removed"] = True
    (evidence / "observed-failure.json").write_text(json.dumps(result, indent=2))
    return result, (plan, submission["blake3"])


def make_fixture(arrays):
    """Generate unique synthetic small files at fixed sizes across all tiers."""
    directory = WORK / ("fixture-" + str(arrays))
    source = directory / "source"
    source.mkdir(parents=True)
    store = Store(directory / "sender")
    names = [f"arrays/a{i:05}" for i in range(arrays)]
    registry = ("\n".join(names) + "\n").encode()
    registry_id, count = store.put_registry(registry)
    spec = {
        "version": 2,
        "partitions": 16,
        "files": [],
        "grids": [],
        "families": [
            {
                "registry": registry_id,
                "count": count,
                "files": ["zarr.json"],
                "grids": [{"prefix": "c", "grid_shape": [99], "separator": "/"}],
            }
        ],
    }
    sizes = []
    unique = set()
    total = 0
    for index, key in enumerate(keys(spec, {registry_id: registry})):
        path = source / key
        path.parent.mkdir(parents=True, exist_ok=True)
        if key.endswith("zarr.json"):
            data = canonical(
                {"zarr_format": 3, "fixture": "synthetic-small-files-v1", "array": key}
            )
        else:
            length = (2048, 18432, 65536, 131072)[index % 4]
            seed = (
                __import__("hashlib")
                .sha256(("synthetic-small-files-v1/" + key).encode())
                .digest()
            )
            data = (seed * ((length + 31) // 32))[:length]
        path.write_bytes(data)
        sizes.append(len(data))
        unique.add(digest(data))
        total += len(data)
    assert len(unique) == len(sizes) and total <= 2 << 30
    publication = store.publish(
        source,
        spec,
        source_id="synthetic-v1-" + str(arrays),
        assume_immutable=True,
        workers=8,
    )
    sorted_sizes = sorted(sizes)
    summary = {
        "synthetic": True,
        "generation": "synthetic-small-files-v1",
        "arrays": arrays,
        "files": len(sizes),
        "unique_objects": len(unique),
        "logical_bytes": total,
        "unique_bytes": total,
        "size_quantiles": {
            str(p): sorted_sizes[min(len(sizes) - 1, int((len(sizes) - 1) * p))]
            for p in (0, 0.25, 0.5, 0.75, 0.95, 1)
        },
        "publication": publication,
        "registry_keys": 1,
        "manifest_keys": publication["partitions"],
        "root_keys": 1,
    }
    target = OUT / "fixtures" / str(arrays)
    target.mkdir(parents=True)
    (target / "descriptor.json").write_bytes(canonical(spec))
    (target / "registry.txt").write_bytes(registry)
    (target / "summary.json").write_text(json.dumps(summary, indent=2))
    return source, store, spec, summary


def sender():
    import secrets, ctypes

    ctypes.CDLL(None).prctl(
        36, 1, 0, 0, 0
    )  # Reap rclone descendants after the deliberate hard kill.
    audit = Audit()
    session = {
        "address": socket.gethostbyname(HOST),
        "access": secrets.token_hex(10),
        "secret": secrets.token_hex(24),
        "server_host": HOST,
        "server_root": str(WORK),
    }
    session["endpoint"] = "http://" + session["address"] + ":13900"
    SECRET_VALUES.extend([session["access"], session["secret"]])
    log = open(WORK / "minio.log", "w")
    client = start_minio(session, log)
    client.create_bucket(Bucket="release-test")
    config = rconfig_for(session)
    source = WORK / "hpa-source"
    store = Store(WORK / "hpa-sender")
    Store(ORIGINAL).restore(A, source, workers=4)
    spec = json.loads((ORIGINAL / "releases" / A).read_bytes())["descriptor"]
    pub = store.publish(
        source, spec, source_id="push-gate-hpa-A", assume_immutable=True, workers=4
    )
    assert pub["release_id"] == A
    _, content, _ = graph(A, lambda key: local_get(store.path, key))
    preserve_graph(store.path, A)
    # Verify audit coverage before using counters for interpretation.
    baseline = audit.snapshot()["counts"]
    client.put_object(Bucket="release-test", Key="audit-probe/key", Body=b"known")
    client.head_object(Bucket="release-test", Key="audit-probe/key")
    assert (
        client.get_object(Bucket="release-test", Key="audit-probe/key")["Body"].read()
        == b"known"
    )
    try:
        client.get_object(Bucket="release-test", Key="audit-probe/missing")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    client.delete_object(Bucket="release-test", Key="audit-probe/key")
    coverage = audit.snapshot()
    delta = {
        key: value - baseline.get(key, 0)
        for key, value in coverage["counts"].items()
        if value - baseline.get(key, 0)
    }
    coverage_pass = delta == {
        "PutObject|200": 1,
        "HeadObject|200": 1,
        "GetObject|200": 1,
        "GetObject|404": 1,
        "DeleteObject|204": 1,
    }
    RESULT["audit_coverage"] = {"known_sdk_sequence": delta, "pass": coverage_pass}
    atomic_file(CONTROL / "session.json", canonical(session))
    atomic_file(CONTROL / "ready.json", canonical({"ready": True}))
    gate_cases = {}
    gate_pass = False
    fixture = None
    for number in range(1, 100):
        request = wait_file(CONTROL / ("request-" + str(number) + ".json"), 900)
        action = request["action"]
        answer = {"action": action, "utc": utc()}
        if action == "audit":
            answer["audit"] = audit.snapshot()
        elif action == "fail-push":
            label = request["case"]
            remote = boundary(client, config, label, "gate/" + label)
            with audit.lock:
                audit.expected[remote.prefix] = set(content) - {"releases/" + A}
            observation, resume = interrupted_push(
                client, remote, store.path, A, content, request["kind"], session, log
            )
            gate_cases[label] = {
                "remote": remote,
                "resume": resume,
                "failure": observation,
            }
            answer["failure"] = observation
            RESULT.setdefault("gate_cases", {})[label] = {"failure": observation}
        elif action == "retry":
            label = request["case"]
            case = gate_cases[label]
            remote = case["remote"]
            before = audit.snapshot()
            receipt = timed(
                "gate-retry-" + label,
                lambda: execute_cli(
                    remote, "push", store.path, A, "retry", resume=case["resume"]
                ),
            )
            assert (
                receipt["plan_blake3"] == case["resume"][1]
                and receipt["reused_keys"] > 0
            )
            assert digest(case["resume"][0].read_bytes()) == case["resume"][1]
            assert not root_absent_if_present(client, remote.prefix, A)
            RESULT["gate_cases"][label]["retry"] = {
                "receipt": receipt,
                "audit_before": before,
                "audit_after": audit.snapshot(),
            }
            answer["receipt"] = receipt
        elif action == "corrupt":
            case = gate_cases[request["case"]]
            key = case["failure"]["complete_payloads"][0]
            data = client.get_object(
                Bucket="release-test", Key=case["remote"].prefix + "/" + key
            )["Body"].read()
            client.put_object(
                Bucket="release-test",
                Key=case["remote"].prefix + "/" + key,
                Body=bytes([data[0] ^ 1]) + data[1:],
            )
            failed = execute_cli(
                case["remote"],
                "push",
                store.path,
                A,
                "corrupt-refusal",
                resume=case["resume"],
                expected=2,
            )
            assert "corrupt remote object" in failed.get("error", "")
            assert root_absent(client, case["remote"].prefix, A)
            assert not (
                OUT / case["remote"].name / "corrupt-refusal/push.receipt.json"
            ).exists()
            RESULT["gate_cases"][request["case"]]["corrupt_refusal"] = {
                "key": key,
                "same_bytes": len(data),
                "result": failed,
            }
            client.delete_object(
                Bucket="release-test", Key=case["remote"].prefix + "/" + key
            )
            answer["manual_recovery"] = (
                "Removed only the known corrupt test object; retry the same pinned plan."
            )
        elif action == "gate-pass":
            assert set(gate_cases) == {"term", "kill", "server", "corrupt"}
            assert all("retry" in value for value in RESULT["gate_cases"].values())
            gate_pass = True
            RESULT["gate_pass_utc"] = utc()
            atomic_file(
                CONTROL / "gate-pass.json", canonical({"pass": True, "utc": utc()})
            )
            answer["pass"] = True
        elif action == "prepare-tier":
            assert gate_pass, "scaling is forbidden until all push cases pass"
            if fixture is not None:
                shutil.rmtree(fixture[0].parent)
            fixture = timed(
                "prepare-tier-" + str(request["arrays"]),
                lambda: make_fixture(request["arrays"]),
            )
            answer["fixture"] = fixture[3]
            RESULT.setdefault("tiers", {})[str(request["arrays"])] = {
                "fixture": fixture[3],
                "trials": [],
            }
        elif action == "tier-push":
            assert gate_pass and fixture is not None
            label = request["label"]
            remote = boundary(client, config, label, "scale/" + label)
            source, retained, descriptor, summary = fixture
            before = audit.snapshot()
            start = time.monotonic()
            value = execute_cli(
                remote,
                "push",
                retained.path,
                summary["publication"]["release_id"],
                "A",
                expected=request.get("expected", 0),
            )
            elapsed = time.monotonic() - start
            after = audit.snapshot()
            trial = {
                "label": label,
                "push": value,
                "push_total_s": elapsed,
                "audit_before": before,
                "audit_after": after,
            }
            RESULT["tiers"][str(summary["arrays"])]["trials"].append(trial)
            if request.get("expected"):
                assert "plan too large" in value.get("error", "")
                assert root_absent(
                    client, remote.prefix, summary["publication"]["release_id"]
                )
            answer["push"] = value
        elif action == "tier-update":
            source, retained, descriptor, summary = fixture
            label = request["label"]
            remote = next(item for name, item in BOUNDARIES if name == label)
            metadata = source / "arrays/a00000/zarr.json"
            value = json.loads(metadata.read_bytes())
            value["revision"] = "B"
            metadata.write_bytes(canonical(value))
            pub = timed(
                "tier-update-publication-" + label,
                lambda: retained.publish(
                    source,
                    descriptor,
                    source_id="synthetic-B-" + label,
                    assume_immutable=True,
                    workers=8,
                    parent=summary["publication"]["release_id"],
                    changed=["arrays/a00000/zarr.json"],
                    assume_complete_changes=True,
                ),
            )
            before = audit.snapshot()
            start = time.monotonic()
            value = execute_cli(remote, "push", retained.path, pub["release_id"], "B")
            RESULT["tiers"][str(summary["arrays"])]["trials"][-1]["update"] = {
                "publication": pub,
                "push": value,
                "push_total_s": time.monotonic() - start,
                "audit_before": before,
                "audit_after": audit.snapshot(),
            }
            answer["release_id"] = pub["release_id"]
            answer["push"] = value
            # Restore A's metadata for the next isolated trial. A remains retained by content.
            old_record = next(
                row
                for row in retained.records(summary["publication"]["release_id"])
                if row[0] == "arrays/a00000/zarr.json"
            )
            metadata.write_bytes(retained.object_path(old_record[2]).read_bytes())
        elif action == "finish":
            answer["audit"] = audit.snapshot()
            RESULT["audit_final"] = answer["audit"]
            atomic_file(CONTROL / ("reply-" + str(number) + ".json"), canonical(answer))
            break
        else:
            raise ValueError("unknown action " + action)
        save()
        atomic_file(CONTROL / ("reply-" + str(number) + ".json"), canonical(answer))
    RESULT["pass"] = gate_pass
    audit.close()


def root_absent_if_present(client, prefix, rid):
    try:
        return root_absent(client, prefix, rid)
    except AssertionError:
        return False


def receiver():
    wait_file(CONTROL / "ready.json", 180)
    session = wait_file(CONTROL / "session.json")
    assert session["server_host"] != HOST
    assert not Path(session["server_root"]).exists()
    with socket.create_connection((session["address"], 13900), timeout=5) as sock:
        RESULT["network"] = {
            "peer": sock.getpeername(),
            "local": sock.getsockname(),
            "server_host": session["server_host"],
        }
    client = client_for(session)
    config = rconfig_for(session)
    number = 0

    def request(action, **fields):
        nonlocal number
        number += 1
        atomic_file(
            CONTROL / ("request-" + str(number) + ".json"),
            canonical({"action": action, **fields}),
        )
        return wait_file(CONTROL / ("reply-" + str(number) + ".json"), 900)

    RESULT["gate_cases"] = {}
    for label, kind in [
        ("term", "term"),
        ("kill", "kill"),
        ("server", "server"),
        ("corrupt", "term"),
    ]:
        observation = request("fail-push", case=label, kind=kind)["failure"]
        remote = boundary(client, config, label, "gate/" + label)
        empty = WORK / ("unpublished-" + label)
        refused = execute_cli(
            remote,
            "pull",
            empty,
            A,
            "unpublished",
            destination=WORK / ("absent-checkout-" + label),
            expected=2,
        )
        assert not (WORK / ("absent-checkout-" + label)).exists()
        assert not (OUT / label / "unpublished/checkout.receipt.json").exists()
        if label == "corrupt":
            request("corrupt", case=label)
        receipt = request("retry", case=label)["receipt"]
        received = timed(
            "gate-pull-" + label,
            lambda: execute_cli(
                remote,
                "pull",
                WORK / ("cache-" + label),
                A,
                "recovered",
                destination=WORK / ("checkout-" + label),
            ),
        )
        assert received["checkout"]["ok"] and received["release_id"] == A
        RESULT["gate_cases"][label] = {
            "failure": observation,
            "unpublished_resolution": refused,
            "retry": receipt,
            "recovery": received,
            "pass": True,
        }
        save()
    assert request("gate-pass")["pass"]
    RESULT["gate_pass_utc"] = utc()
    save()
    # Validate audit capture of rclone traffic as well as SDK calls.
    probe = WORK / "audit-rclone"
    probe.write_bytes(b"known-rclone")
    before = request("audit")["audit"]
    subprocess.run(
        [
            "/usr/bin/rclone",
            "--config",
            str(config),
            "copyto",
            str(probe),
            "s3:release-test/audit-probe/rclone",
        ],
        check=True,
    )
    copied = subprocess.check_output(
        [
            "/usr/bin/rclone",
            "--config",
            str(config),
            "cat",
            "s3:release-test/audit-probe/rclone",
        ]
    )
    assert copied == probe.read_bytes()
    after = request("audit")["audit"]
    covered = (
        after["clients"].get("rclone", 0) > before["clients"].get("rclone", 0)
        and after["counts"].get("PutObject|200", 0)
        >= before["counts"].get("PutObject|200", 0) + 1
        and after["counts"].get("GetObject|200", 0)
        >= before["counts"].get("GetObject|200", 0) + 1
    )
    RESULT["audit_rclone_validation"] = {
        "before": before,
        "after": after,
        "pass": covered,
    }
    RESULT["tiers"] = {}
    for arrays, trials in [(10, 3), (99, 1), (100, 1)]:
        info = request("prepare-tier", arrays=arrays)["fixture"]
        RESULT["tiers"][str(arrays)] = {"fixture": info, "trials": []}
        rid = info["publication"]["release_id"]
        for trial in range(trials):
            label = "n" + str(info["unique_objects"]) + "-t" + str(trial)
            response = request(
                "tier-push", label=label, expected=2 if arrays == 100 else 0
            )
            if arrays == 100:
                RESULT["tiers"][str(arrays)]["limit_result"] = response["push"]
                save()
                continue
            remote = boundary(client, config, label, "scale/" + label)
            trial_dir = WORK / label
            trial_dir.mkdir()
            before = request("audit")["audit"]
            cold = timed(
                label + "-cold-pull",
                lambda: execute_cli(
                    remote,
                    "pull",
                    trial_dir / "cache",
                    rid,
                    "cold",
                    destination=trial_dir / "checkout",
                ),
            )
            after = request("audit")["audit"]
            start = time.monotonic()
            warm = execute_cli(
                remote,
                "pull",
                trial_dir / "cache",
                rid,
                "warm",
                destination=trial_dir / "warm-checkout",
            )
            warm_s = time.monotonic() - start
            after_warm = request("audit")["audit"]
            updated = request("tier-update", label=label)
            bid = updated["release_id"]
            before_update = request("audit")["audit"]
            start = time.monotonic()
            update = execute_cli(
                remote,
                "pull",
                trial_dir / "cache",
                bid,
                "update",
                destination=trial_dir / "B-checkout",
            )
            update_s = time.monotonic() - start
            after_update = request("audit")["audit"]
            # Recover A from the same remote after B exists, with an independent empty cache.
            original = execute_cli(
                remote,
                "pull",
                trial_dir / "A-again-cache",
                rid,
                "A-after-B",
                destination=trial_dir / "A-again-checkout",
            )
            assert (
                cold["checkout"]["files"] == info["files"]
                and warm["fetch"]["downloaded_objects"] == 0
            )
            assert (
                update["fetch"]["downloaded_objects"] == 1
                and original["release_id"] == rid
            )
            RESULT["tiers"][str(arrays)]["trials"].append(
                {
                    "label": label,
                    "cold": cold,
                    "warm": warm,
                    "warm_s": warm_s,
                    "update": update,
                    "update_s": update_s,
                    "A_after_B": original,
                    "audit_before_cold": before,
                    "audit_after_cold": after,
                    "audit_before_warm": after,
                    "audit_after_warm": after_warm,
                    "audit_before_update": before_update,
                    "audit_after_update": after_update,
                    "audit_after_A_recovery": request("audit")["audit"],
                }
            )
            save()
            shutil.rmtree(trial_dir)
    # Descriptor planning demonstrates the next requested count without creating 100k files.
    RESULT["unsupported_100000"] = {
        "requested_objects": 100000,
        "executed": False,
        "reason": "Current graph limit is 10000 files and transfer-plan limit is 10000 entries; no limit was increased.",
    }
    final = request("finish")
    RESULT["audit_final"] = final["audit"]
    RESULT["pass"] = True


def main():
    global WORK
    os.umask(0o077)

    def terminate(_signal, _frame):
        raise SystemExit("allocation terminated")

    signal.signal(signal.SIGTERM, terminate)
    for name in list(os.environ):
        if name.startswith(("AWS_", "RCLONE_", "HERMES_")) or name.lower() in (
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ):
            del os.environ[name]
    os.environ.update(
        AWS_EC2_METADATA_DISABLED="true",
        AWS_CONFIG_FILE="/dev/null",
        AWS_SHARED_CREDENTIALS_FILE="/dev/null",
    )
    WORK = Path(tempfile.mkdtemp(prefix="dv-push-scale-" + str(RANK) + "-", dir="/tmp"))
    RESULT.update(
        start_utc=utc(),
        work=str(WORK),
        filesystem=subprocess.check_output(["df", "-T", str(WORK)], text=True),
        disk_free_before=shutil.disk_usage(WORK).free,
        free_inodes_before=os.statvfs(WORK).f_favail,
        code_base=subprocess.check_output(
            ["git", "-C", str(ROOT / "zarrsum"), "rev-parse", "HEAD"], text=True
        ).strip(),
        source_sha256={
            str(path.relative_to(ROOT / "zarrsum")): __import__("hashlib")
            .sha256(path.read_bytes())
            .hexdigest()
            for path in (ROOT / "zarrsum/src/zarrsum").glob("*.py")
        },
    )
    assert (
        RESULT["disk_free_before"] > 12 << 30 and RESULT["free_inodes_before"] > 250000
    )
    commands = []
    original_popen = subprocess.Popen

    def logged(args, *a, **kwargs):
        if isinstance(args, list):
            commands.append({"utc": utc(), "argv": args})
        return original_popen(args, *a, **kwargs)

    subprocess.Popen = logged
    stop = threading.Event()
    peaks = {"logical_bytes": 0, "files": 0, "process_tree_rss_bytes": 0}

    def monitor():
        while not stop.wait(0.25):
            size = count = 0
            for path in WORK.rglob("*"):
                try:
                    if path.is_file():
                        size += path.stat().st_size
                        count += 1
                except FileNotFoundError:
                    pass
            peaks["logical_bytes"] = max(peaks["logical_bytes"], size)
            peaks["files"] = max(peaks["files"], count)
            try:
                processes = [psutil.Process()] + psutil.Process().children(
                    recursive=True
                )
                peaks["process_tree_rss_bytes"] = max(
                    peaks["process_tree_rss_bytes"],
                    sum(p.memory_info().rss for p in processes if p.is_running()),
                )
            except psutil.Error:
                pass
            if size > 12 << 30 or count > 1000000:
                os.kill(os.getpid(), signal.SIGTERM)

    local_temp = WORK / "process-temporary"
    local_temp.mkdir()
    os.environ["TMPDIR"] = str(local_temp)
    tempfile.tempdir = str(local_temp)
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    RESULT["budgets"] = {
        "runtime_bytes_per_host": 12 << 30,
        "files_per_host": 1000000,
        "unique_payload_per_tier": 2 << 30,
    }
    try:
        (sender if RANK == 0 else receiver)()
    except BaseException:
        RESULT["pass"] = False
        RESULT["error"] = traceback.format_exc()
        atomic_file(
            CONTROL / "abort.json",
            canonical({"host": HOST, "rank": RANK, "error": RESULT["error"]}),
        )
        raise
    finally:
        stop_minio()
        stop.set()
        thread.join(timeout=2)
        RESULT["boundaries"] = {
            name: {
                "events": item.events,
                "transfers": item.transfers,
                "sdk_calls": item.requests,
            }
            for name, item in BOUNDARIES
        }
        RESULT["peaks_sampled_250ms"] = peaks
        RESULT["end_utc"] = utc()
        (OUT / "commands.json").write_text(json.dumps(commands, indent=2))
        if (WORK / "minio.log").exists():
            log = (WORK / "minio.log").read_text()
            for value in SECRET_VALUES:
                log = log.replace(value, "<redacted>")
            (OUT / "minio.log").write_text(log)
        shutil.rmtree(WORK)
        RESULT["node_scratch_removed"] = not WORK.exists()
        save()
        if RANK == 0:
            (CONTROL / "session.json").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
