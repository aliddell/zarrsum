"""Exercise a pinned release through S3 between two exclusive compute nodes.

Shared storage carries code, credentials, control messages, and evidence. All payload
stores and checkouts use each node's private /tmp directory.
"""

from __future__ import annotations

import concurrent.futures
import json
import multiprocessing
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
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import psutil

from zarrsum.releases import Store, canonical, digest, keys, atomic_file
from zarrsum.remotes import Boundary, graph, local_get, Aliases, REPOSITORY

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
    return path


def boundary(client, config, name, prefix="retained"):
    item = Boundary(
        client, config, OUT / name, prefix, allowed_endpoints={client.meta.endpoint_url}
    )
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


def sender():
    import secrets

    address = socket.gethostbyname(HOST)
    session = {
        "address": address,
        "endpoint": "http://" + address + ":13900",
        "access": secrets.token_hex(10),
        "secret": secrets.token_hex(24),
        "server_host": HOST,
        "server_root": str(WORK),
    }
    SECRET_VALUES.extend([session["access"], session["secret"]])
    log = open(WORK / "minio.log", "w")
    client = timed("server-start", lambda: start_minio(session, log))
    client.create_bucket(Bucket="release-test")
    config = rconfig_for(session)
    remote = boundary(client, config, "main")
    source = WORK / "source"
    store = Store(WORK / "sender")
    timed("source-prepare", lambda: Store(ORIGINAL).restore(A, source, workers=4))
    spec = json.loads((ORIGINAL / "releases" / A).read_bytes())["descriptor"]
    publication = timed(
        "publish-A",
        lambda: store.publish(
            source, spec, source_id="two-host-A", assume_immutable=True, workers=4
        ),
    )
    assert publication["release_id"] == A
    RESULT["publication_A"] = publication
    RESULT["push_A"] = timed("push-A", lambda: remote.push(store.path, A, "A"))
    RESULT["A_counts"] = preserve_graph(store.path, A)
    rs = Store(WORK / "registry-store")
    registry = b"array-a\narray-b\n"
    rid, count = rs.put_registry(registry)
    rspec = {
        "version": 2,
        "partitions": 4,
        "files": ["metadata.json"],
        "grids": [],
        "families": [
            {
                "registry": rid,
                "count": count,
                "files": ["zarr.json"],
                "grids": [{"prefix": "c", "grid_shape": [2], "separator": "/"}],
            }
        ],
    }
    rsource = WORK / "registry-source"
    rsource.mkdir()
    for key in keys(rspec, {rid: registry}):
        path = rsource / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"shared-payload" if "/c/" in key else b"{}")
    rp = rs.publish(rsource, rspec, source_id="registry-v1", assume_immutable=True)
    reg = boundary(client, config, "registry", "registry")
    RESULT["registry_push"] = reg.push(rs.path, rp["release_id"], "registry")
    session["registry_id"] = rp["release_id"]
    preserve_graph(rs.path, rp["release_id"])
    store.path.rename(WORK / "sender-hidden")
    source.rename(WORK / "source-hidden")
    RESULT["sender_hidden_before_receiver"] = True
    RESULT["endpoint"] = session["endpoint"]
    RESULT["address"] = address
    atomic_file(CONTROL / "session.json", canonical(session))
    atomic_file(CONTROL / "ready.json", canonical({"ready": True}))
    save()
    B = None
    for number in range(1, 20):
        request = wait_file(CONTROL / ("request-" + str(number) + ".json"), 300)
        action = request["action"]
        answer = {"action": action, "utc": utc()}
        if action == "publish-B":
            hidden = WORK / "sender-hidden"
            hidden.rename(store.path)
            (WORK / "source-hidden").rename(source)
            value = json.loads((source / "provenance.json").read_bytes())
            value["local_s3_revision"] = "metadata-only-B"
            (source / "provenance.json").write_bytes(canonical(value))
            pub = timed(
                "publish-B",
                lambda: store.publish(
                    source,
                    spec,
                    source_id="two-host-B",
                    assume_immutable=True,
                    workers=4,
                    parent=A,
                    changed=["provenance.json"],
                    assume_complete_changes=True,
                ),
            )
            B = pub["release_id"]
            assert B == "a147f08d5e8708647c6dea2001c03ca6cc271a69ce018baa1949d14d9fb4c8cf"
            RESULT["publication_B"] = pub
            RESULT["push_B"] = timed("push-B", lambda: remote.push(store.path, B, "B"))
            preserve_graph(store.path, B)
            store.path.rename(hidden)
            source.rename(WORK / "source-hidden")
            answer["release_id"] = B
        elif action == "fault-prefixes":
            hidden = WORK / "sender-hidden"
            _, contents, _ = graph(A, lambda key: local_get(hidden, key))
            obj = next(
                key
                for key in contents
                if key.startswith("objects/") and contents[key][0] > 100000
            )
            for label in ("missing", "corrupt"):
                bad = boundary(client, config, label, "faults/" + label)
                bad.push(hidden, A, "seed")
                if label == "missing":
                    client.delete_object(
                        Bucket="release-test", Key=bad.prefix + "/" + obj
                    )
                else:
                    data = bad.get(obj)
                    bad.put(obj, bytes([data[0] ^ 1]) + data[1:])
            answer["object"] = obj
        elif action == "restart":
            stop_minio()
            timed("server-restart", lambda: start_minio(session, log))
            assert remote.get("releases/" + A) == local_get(
                WORK / "sender-hidden", "releases/" + A
            )
        elif action == "stop":
            stop_minio()
            answer["stopped"] = True
        elif action == "finish":
            stop_minio()
            atomic_file(CONTROL / ("reply-" + str(number) + ".json"), canonical(answer))
            break
        else:
            raise ValueError("unknown request")
        atomic_file(CONTROL / ("reply-" + str(number) + ".json"), canonical(answer))
    RESULT["pass"] = True


def receiver():
    wait_file(CONTROL / "ready.json")
    session = wait_file(CONTROL / "session.json")
    SECRET_VALUES.extend([session["access"], session["secret"]])
    assert session["server_host"] != HOST
    assert not Path(session["server_root"]).exists(), (
        "server /tmp is shared with receiver"
    )
    with socket.create_connection((session["address"], 13900), timeout=5) as stream:
        peer = stream.getpeername()
        local = stream.getsockname()
    assert peer[0] == session["address"] and not peer[0].startswith("127.")
    health = urllib.request.urlopen(
        session["endpoint"] + "/minio/health/live", timeout=5
    ).status
    assert health == 200
    RESULT["network"] = {
        "peer": peer,
        "local": local,
        "health_http": health,
        "endpoint": session["endpoint"],
        "server_host": session["server_host"],
        "distinct_hosts": True,
        "server_tmp_absent": True,
    }
    client = client_for(session)
    config = rconfig_for(session)
    remote = boundary(client, config, "main")
    number = 0

    def request(action):
        nonlocal number
        number += 1
        atomic_file(
            CONTROL / ("request-" + str(number) + ".json"),
            canonical({"action": action}),
        )
        return wait_file(CONTROL / ("reply-" + str(number) + ".json"))

    denied = [str(ORIGINAL.parent), session["server_root"]]

    def audit(event, args):
        if event == "open" and isinstance(args[0], (str, bytes, os.PathLike)):
            path = os.fsdecode(args[0])
            if any(
                path == prefix or path.startswith(prefix + "/") for prefix in denied
            ):
                raise PermissionError(
                    "receiver cannot open sender or shared source data"
                )

    sys.addaudithook(audit)
    # Signed S3 requests and rclone both use the allocated peer address.
    assert (
        client.head_object(Bucket="release-test", Key="retained/releases/" + A)[
            "ContentLength"
        ]
        > 0
    )
    nonce = "network-probe/root"

    def race(body):
        try:
            response = client.put_object(
                Bucket="release-test", Key=nonce, Body=body, IfNoneMatch="*"
            )
            return [response["ResponseMetadata"]["HTTPStatusCode"], body.decode()]
        except ClientError as error:
            return [error.response["ResponseMetadata"]["HTTPStatusCode"], body.decode()]

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        raced = list(pool.map(race, [b"writer-one", b"writer-two"]))
    assert sorted(row[0] for row in raced) == [200, 412]
    stored = client.get_object(Bucket="release-test", Key=nonce)["Body"].read()
    assert stored == next(row[1].encode() for row in raced if row[0] == 200)
    RESULT["checks"]["network-conditional-root"] = {"pass": True, "outcomes": raced}
    read = subprocess.run(
        [
            "/mnt/main0/home/aliddell/.bin/rclone",
            "--config",
            str(config),
            "cat",
            "s3:release-test/retained/releases/" + A,
        ],
        capture_output=True,
        timeout=30,
        check=True,
    )
    assert digest(read.stdout) == A
    RESULT["checks"]["network-sdk-and-rclone"] = {"pass": True}
    aliases = Aliases(WORK / "aliases.json", allowed_endpoints={session["endpoint"]})
    aliases.add(
        "allocated-fixture",
        {
            "endpoint": session["endpoint"],
            "region": "garage",
            "bucket": "release-test",
            "prefix": "retained",
            "rclone_remote": "s3",
            "format": REPOSITORY["format"],
        },
    )
    assert aliases.list()["allocated-fixture"]["endpoint"] == session["endpoint"]
    cache = WORK / "receiver"
    checkout = WORK / "checkout-A"
    RESULT["cold_fetch_A"] = timed(
        "cold-fetch-A", lambda: remote.fetch(A, cache, "cold-A")
    )
    RESULT["checkout_A"] = timed(
        "checkout-A", lambda: remote.checkout(cache, A, checkout, "A-checkout")
    )
    RESULT["cold_fetch_A_trial2"] = timed(
        "cold-fetch-A-trial2", lambda: remote.fetch(A, WORK / "receiver2", "cold-A2")
    )
    timed(
        "checkout-A-trial2",
        lambda: remote.checkout(
            WORK / "receiver2", A, WORK / "checkout-A2", "A2-checkout"
        ),
    )
    RESULT["warm_fetch_A"] = timed(
        "warm-fetch-A", lambda: remote.fetch(A, cache, "warm-A")
    )
    timed(
        "warm-checkout-A",
        lambda: remote.checkout(cache, A, WORK / "warm-checkout-A", "warm-checkout"),
    )
    wrong = WORK / "wrong-destination"
    wrong.mkdir()
    (wrong / "keep").write_bytes(b"original")
    failure(
        "existing-destination-preserved",
        lambda: remote.checkout(cache, A, wrong, "wrong"),
    )
    assert (wrong / "keep").read_bytes() == b"original"
    B = request("publish-B")["release_id"]
    RESULT["fetch_B"] = timed("fetch-B", lambda: remote.fetch(B, cache, "B"))
    timed(
        "checkout-B",
        lambda: remote.checkout(cache, B, WORK / "checkout-B", "B-checkout"),
    )
    RESULT["A_after_B"] = timed(
        "A-after-B", lambda: remote.fetch(A, WORK / "receiver-after-B", "A-after-B")
    )
    reg = boundary(client, config, "registry", "registry")
    RESULT["registry_fetch"] = timed(
        "registry-fetch",
        lambda: reg.fetch(session["registry_id"], WORK / "registry-cache", "registry"),
    )
    timed(
        "registry-checkout",
        lambda: reg.checkout(
            WORK / "registry-cache",
            session["registry_id"],
            WORK / "registry-checkout",
            "registry",
        ),
    )
    interrupted = boundary(client, config, "interruption")
    badcache = WORK / "interrupted-cache"
    badcheckout = WORK / "interrupted-checkout"

    def child_fetch():
        os.setsid()
        os.environ["HERMES_RUNNER_DEFAULT_BWLIMIT"] = "256K"
        interrupted.fetch(A, badcache, "interrupted")
        interrupted.checkout(badcache, A, badcheckout, "interrupted-checkout")

    child = multiprocessing.get_context("fork").Process(target=child_fetch)
    child.start()
    deadline = time.monotonic() + 45
    completed = []
    active = False
    while time.monotonic() < deadline and child.is_alive():
        plan = OUT / "interruption/interrupted.plan.json"
        if plan.exists():
            data = json.loads(plan.read_bytes())
            incoming = Path(data["destination"])
            completed = []
            for key, length, value in data["entries"]:
                path = incoming / key
                if (
                    path.is_file()
                    and path.stat().st_size == length
                    and digest(path.read_bytes()) == value
                ):
                    completed.append([key, length, value])
            processes = psutil.Process(child.pid).children(recursive=True)
            active = any(
                "rclone" in p.name() and "copy" in p.cmdline() for p in processes
            )
            if completed and active:
                break
        time.sleep(0.05)
    assert completed and active, (
        "no completed cross-node payload while rclone was active"
    )
    os.killpg(child.pid, signal.SIGKILL)
    child.join(timeout=10)
    assert child.exitcode == -signal.SIGKILL
    assert not badcheckout.exists() and not (badcache / "releases" / A).exists()
    RESULT["interruption"] = {
        "completed_objects": len(completed),
        "completed_bytes": sum(row[1] for row in completed),
        "child_exit": child.exitcode,
        "local_root_absent": True,
        "checkout_absent": True,
        "partial_location": str(incoming),
    }
    # Fetch admission starts only after download completion. Retry uses a new private area.
    RESULT["retry_fetch"] = timed(
        "retry-fetch-A", lambda: interrupted.fetch(A, badcache, "retry")
    )
    assert (
        RESULT["retry_fetch"]["release_id"] == A
        and RESULT["retry_fetch"]["cache_hits"] == 0
    )
    timed(
        "retry-checkout-A",
        lambda: interrupted.checkout(badcache, A, badcheckout, "retry-checkout"),
    )
    RESULT["checks"]["active-cross-node-interruption-and-retry"] = {
        "pass": True,
        "partial_reuse": False,
        "redownloaded_objects": RESULT["retry_fetch"]["downloaded_objects"],
    }
    request("restart")
    RESULT["restart_fetch"] = timed(
        "restart-fetch-A", lambda: remote.fetch(A, WORK / "restart-cache", "restart")
    )
    timed(
        "restart-checkout-A",
        lambda: remote.checkout(
            WORK / "restart-cache", A, WORK / "restart-checkout", "restart-checkout"
        ),
    )
    request("fault-prefixes")
    for label in ("missing", "corrupt"):
        bad = boundary(client, config, label, "faults/" + label)
        destination = WORK / (label + "-cache")
        failure(label + "-remote-object", lambda: bad.fetch(A, destination, "bad"))
        assert not (destination / "releases" / A).exists()
        assert not (WORK / (label + "-checkout")).exists()
    request("stop")
    try:
        socket.create_connection((session["address"], 13900), timeout=2)
    except OSError as error:
        RESULT["server_stopped_observation"] = type(error).__name__
    else:
        raise AssertionError("S3 server still reachable")
    code = """import json,sys
from pathlib import Path
from zarrsum.releases import Store
from zarrsum.remotes import Boundary
import os
blocked=json.loads(sys.argv[4])
def audit(event,args):
    if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
        p=os.fsdecode(args[0])
        if any(p==x or p.startswith(x+'/') for x in blocked):
            raise PermissionError('sender or shared source denied in offline process')
sys.addaudithook(audit)
store=Store(sys.argv[1]);destination=Path(sys.argv[3]);private=destination.with_name('.offline-private')
store.restore(sys.argv[2],private,workers=4)
verdict=store.verify(sys.argv[2],private,workers=4)
os.rename(private,destination)
print(json.dumps(verdict))
"""
    final = WORK / "offline-checkout"
    process = timed(
        "offline-fresh-process-checkout",
        lambda: subprocess.run(
            [sys.executable, "-c", code, str(cache), A, str(final), json.dumps(denied)],
            capture_output=True,
            text=True,
            check=True,
        ),
    )
    RESULT["offline_fresh_verify"] = json.loads(process.stdout)
    record = next(Store(cache).records(A))
    path = final / record[0]
    raw = path.read_bytes()
    path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    failure(
        "post-checkout-corruption", lambda: Store(cache).verify(A, final, workers=4)
    )
    aliases.remove("allocated-fixture")
    assert not aliases.list()
    preserve_graph(cache, A)
    preserve_graph(cache, B)
    request("finish")
    RESULT["release_B"] = B
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
        HERMES_MAX_RETRIES="0",
        HERMES_RCLONE_MSF_TRANSFERS="8",
        HERMES_RCLONE_MSF_CHECKERS="8",
        HERMES_RCLONE_S3_CHUNK_SIZE="5M",
        HERMES_RCLONE_S3_UPLOAD_CONCURRENCY="2",
    )
    WORK = Path(tempfile.mkdtemp(prefix="dv-two-host-" + str(RANK) + "-", dir="/tmp"))
    RESULT.update(
        start_utc=utc(),
        work=str(WORK),
        filesystem=subprocess.check_output(["df", "-T", str(WORK)], text=True),
        disk_free_before=shutil.disk_usage(WORK).free,
        free_inodes_before=os.statvfs(WORK).f_favail,
        code_commits={
            name: subprocess.check_output(
                ["git", "-C", str(ROOT / name), "rev-parse", "HEAD"], text=True
            ).strip()
            for name in ("zarrsum", "hermes")
        },
    )
    assert RESULT["disk_free_before"] > 2 << 30 and RESULT["free_inodes_before"] > 20000
    commands = []
    original_popen = subprocess.Popen

    def logged(args, *a, **kwargs):
        if isinstance(args, list) and (
            "rclone" in str(args[0]) or str(args[0]) == sys.executable
        ):
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
            if size > 1 << 30 or count > 20000:
                os.kill(os.getpid(), signal.SIGTERM)

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
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
