"""Run the bounded local transfer and isolated fault tests on a compute node."""

from __future__ import annotations
import concurrent.futures
from dataclasses import asdict
import json
import multiprocessing
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback

import psutil
from zarrsum.releases import Store, canonical, digest, atomic_file, keys
from zarrsum.remotes import Boundary, Aliases, REPOSITORY, graph, local_get, object_key
from local_s3_probe import ROOT, minio_fixture, probe

ORIGINAL = Path("/mnt/main0/home/aliddell/scratch/representative-20261117/store")
A = "729ce82fb94fc7eb9b4bea7ef688dd1374015d672d2c3294fe4deb2a14b5ee34"
RESULT = {}


def main():
    os.umask(0o077)
    for key in list(os.environ):
        if key.startswith(("AWS_", "RCLONE_", "HERMES_")):
            del os.environ[key]
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
    out = ROOT / ("run-" + os.environ["SLURM_JOB_ID"])
    out.mkdir()
    work = Path(tempfile.mkdtemp(prefix="dv-s3-roundtrip-", dir="/tmp"))
    result = {
        "start_utc": utc(),
        "host": os.uname().nodename,
        "work": str(work),
        "tests": {},
        "timings": [],
        "settings": {
            "qos": "low",
            "exclusive": True,
            "transfers": 8,
            "checkers": 8,
            "plan_batch": 64,
            "disk_budget_bytes": 1 << 30,
            "inode_budget": 20000,
            "cache": "empty receiver on disk; coordinator warm; rclone processes new; OS warm",
        },
    }
    RESULT.update(result)
    command_log = []
    original_popen = subprocess.Popen

    def logged_popen(args, *a, **kw):
        if isinstance(args, list) and (
            "rclone" in str(args[0]) or str(args[0]) == sys.executable
        ):
            command_log.append({"utc": utc(), "argv": args})
        return original_popen(args, *a, **kw)

    subprocess.Popen = logged_popen
    boundaries = []
    server = None
    monitor_stop = threading.Event()
    peaks = {
        "runtime_logical_bytes": 0,
        "runtime_files": 0,
        "process_tree_rss_bytes": 0,
    }

    def monitor():
        while not monitor_stop.wait(0.25):
            size = files = 0
            for p in work.rglob("*"):
                try:
                    if p.is_file():
                        size += p.stat().st_size
                        files += 1
                except FileNotFoundError:
                    pass
            peaks["runtime_logical_bytes"] = max(peaks["runtime_logical_bytes"], size)
            peaks["runtime_files"] = max(peaks["runtime_files"], files)
            try:
                processes = [psutil.Process()] + psutil.Process().children(
                    recursive=True
                )
                rss = sum(p.memory_info().rss for p in processes if p.is_running())
                peaks["process_tree_rss_bytes"] = max(
                    peaks["process_tree_rss_bytes"], rss
                )
            except psutil.Error:
                pass
            if size > 1 << 30 or files > 20000:
                os.kill(os.getpid(), signal.SIGTERM)

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()

    def saved():
        (out / "result.json").write_text(json.dumps(result, indent=2, default=str))

    def timed(name, fn):
        start = time.monotonic()
        value = fn()
        result["timings"].append(
            {"operation": name, "wall_s": time.monotonic() - start, "utc": utc()}
        )
        saved()
        return value

    def boundary(name, prefix):
        b = Boundary(client, rconfig, out / name, prefix)
        boundaries.append((name, b))
        return b

    def failure(name, fn, absent=None):
        start = time.monotonic()
        try:
            fn()
        except (ValueError, FileNotFoundError, FileExistsError) as e:
            if absent is not None:
                assert not absent.exists()
            result["tests"][name] = {
                "pass": True,
                "exception": type(e).__name__,
                "detail": str(e)[:600],
                "wall_s": time.monotonic() - start,
                "utc": utc(),
            }
            saved()
            return
        raise AssertionError("negative test did not fail: " + name)

    try:
        result["filesystem_before"] = {
            "tmp": dict(zip(("total", "used", "free"), shutil.disk_usage(work))),
            "statvfs": {"free_inodes": os.statvfs(work).f_favail},
        }
        client, server, restart, rconfig, log = timed(
            "minio-setup", lambda: minio_fixture(work / "service")
        )
        result["probe"] = timed("http-contract-probe", lambda: probe(client, rconfig))
        assert (
            result["probe"]["conditional_pass"]
            and result["probe"]["conditional_stored_matches_winner"]
        )
        testfile = work / "rclone-put"
        testfile.write_bytes(b"rclone-wrote-this")
        p = subprocess.run(
            [
                "/mnt/main0/home/aliddell/.bin/rclone",
                "--config",
                str(rconfig),
                "copyto",
                str(testfile),
                "s3:release-test/probe/rclone-put",
            ],
            capture_output=True,
            timeout=30,
        )
        assert (
            p.returncode == 0
            and client.get_object(Bucket="release-test", Key="probe/rclone-put")[
                "Body"
            ].read()
            == testfile.read_bytes()
        )
        result["probe"]["rclone_put"] = "pass"
        sender = work / "sender"
        source = work / "source"
        receiver = work / "receiver"
        checkout = work / "checkout-A"
        timed(
            "prepare-source-from-retained-A",
            lambda: Store(ORIGINAL).restore(A, source, workers=4),
        )
        spec = json.loads((ORIGINAL / "releases" / A).read_bytes())["descriptor"]
        store = Store(sender)
        publication = timed(
            "source-publication-A",
            lambda: store.publish(
                source,
                spec,
                source_id="local-s3-A-"
                + digest((source / "provenance.json").read_bytes()),
                assume_immutable=True,
                workers=4,
            ),
        )
        assert publication["release_id"] == A
        result["publication_A"] = publication
        b = boundary("main", "retained")
        alias = Aliases(work / "aliases.json")
        config = {
            "endpoint": "http://127.0.0.1:13900",
            "region": "garage",
            "bucket": "release-test",
            "prefix": "retained",
            "rclone_remote": "s3",
            "format": REPOSITORY["format"],
        }
        alias.add("local-fixture", config)
        assert alias.list() == {"local-fixture": config}
        failure(
            "reject-nonlocal-endpoint",
            lambda: alias.add(
                "bad", {**config, "endpoint": "https://s3.amazonaws.com"}
            ),
        )
        result["push_A"] = timed("push-A-total", lambda: b.push(sender, A, "A"))
        hidden = work / "sender-unavailable"
        sender.rename(hidden)
        source.rename(work / "source-unavailable")
        denied = [
            str(ORIGINAL),
            str(sender),
            str(hidden),
            str(work / "source-unavailable"),
            str(source),
        ]
        block = {"active": True}

        def audit(event, args):
            if (
                block["active"]
                and event == "open"
                and isinstance(args[0], (str, bytes, os.PathLike))
            ):
                path = os.fsdecode(args[0])
                if any(path == p or path.startswith(p + "/") for p in denied):
                    raise PermissionError(
                        "sender and original source denied during recovery"
                    )

        sys.addaudithook(audit)
        result["fetch_A"] = timed(
            "cold-fetch-A-total", lambda: b.fetch(A, receiver, "cold-A")
        )
        result["checkout_A"] = timed(
            "checkout-A-total", lambda: b.checkout(receiver, A, checkout, "A-checkout")
        )
        result["warm_A"] = timed(
            "warm-fetch-A-total", lambda: b.fetch(A, receiver, "warm-A")
        )
        timed(
            "warm-checkout-A-total",
            lambda: b.checkout(receiver, A, work / "warm-checkout-A", "warm-checkout"),
        )
        clean2 = work / "receiver-trial2"
        result["cold_A_trial2"] = timed(
            "cold-fetch-A-trial2", lambda: b.fetch(A, clean2, "cold-A-2")
        )
        result["sender_denied_during_recovery"] = True
        block["active"] = False
        hidden.rename(sender)
        (work / "source-unavailable").rename(source)
        metadata_key = "provenance.json"
        changed = json.loads((source / metadata_key).read_bytes())
        changed["local_s3_revision"] = "metadata-only-B"
        (source / metadata_key).write_bytes(canonical(changed))
        publication_b = timed(
            "source-publication-B",
            lambda: store.publish(
                source,
                spec,
                source_id="local-s3-B-" + digest((source / metadata_key).read_bytes()),
                assume_immutable=True,
                workers=4,
                parent=A,
                changed=[metadata_key],
                assume_complete_changes=True,
            ),
        )
        B = publication_b["release_id"]
        result["publication_B"] = publication_b
        result["push_B"] = timed("push-B-total", lambda: b.push(sender, B, "B"))
        result["fetch_B"] = timed(
            "fetch-B-warm-cache", lambda: b.fetch(B, receiver, "B-fetch")
        )
        timed(
            "checkout-B-total",
            lambda: b.checkout(receiver, B, work / "checkout-B", "B-checkout"),
        )
        result["A_still_retrievable"] = timed(
            "A-recovery-after-B",
            lambda: b.fetch(A, work / "receiver-after-B", "A-after-B"),
        )
        alias.remove("local-fixture")
        assert alias.list() == {}
        assert b.get("releases/" + A) == (sender / "releases" / A).read_bytes()
        result["tests"]["alias-remove-preserves-data"] = {"pass": True}
        # A registry fixture exercises metadata that the representative descriptor lacks.
        rs = Store(work / "registry-sender")
        reg = b"array-a\narray-b\n"
        rid, count = rs.put_registry(reg)
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
        rsource = work / "registry-source"
        rsource.mkdir()
        for key in keys(rspec, {rid: reg}):
            path = rsource / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"shared-payload" if "/c/" in key else b"{}")
        rp = rs.publish(
            rsource, rspec, source_id="registry-fixture-v1", assume_immutable=True
        )
        r = boundary("registry", "registry")
        result["registry_push"] = r.push(rs.path, rp["release_id"], "registry")
        r.fetch(rp["release_id"], work / "registry-receiver", "registry")
        r.checkout(
            work / "registry-receiver",
            rp["release_id"],
            work / "registry-checkout",
            "registry",
        )
        result["tests"]["registry-and-repeated-payload"] = {
            "pass": True,
            "publication": rp,
        }
        # All fault namespaces are separate from retained A and B.
        _, content, records = graph(A, lambda k: local_get(sender, k))
        obj = next(
            k for k in content if k.startswith("objects/") and content[k][0] > 100000
        )

        def seed(name):
            x = boundary(name, "faults/" + name)
            x.push(sender, A, "seed")
            return x

        missing = seed("missing-remote")
        client.delete_object(Bucket="release-test", Key=missing.prefix + "/" + obj)
        failure(
            "missing-required-remote",
            lambda: missing.fetch(A, work / "missing-receiver", "bad"),
        )
        assert not (work / "missing-receiver" / "releases" / A).exists()
        corrupt = seed("corrupt-remote")
        original = corrupt.get(obj)
        corrupt.put(obj, bytes([original[0] ^ 1]) + original[1:])
        failure(
            "same-size-corrupt-remote",
            lambda: corrupt.fetch(A, work / "corrupt-receiver", "bad"),
        )
        assert not (work / "corrupt-receiver" / "releases" / A).exists()
        cache = work / "bad-cache"
        shutil.copytree(receiver, cache)
        path = cache / obj
        data = path.read_bytes()
        path.write_bytes(bytes([data[0] ^ 1]) + data[1:])
        failure("same-size-corrupt-cache", lambda: b.fetch(A, cache, "bad-cache"))
        failure(
            "corrupt-cache-no-checkout",
            lambda: b.checkout(cache, A, work / "bad-cache-checkout", "bad-cache"),
            work / "bad-cache-checkout",
        )
        wrong = work / "wrong-destination"
        wrong.mkdir()
        wrong_key = next(iter(records))
        wrong_file = wrong / wrong_key
        wrong_file.parent.mkdir(parents=True, exist_ok=True)
        before = b"X" * records[wrong_key][1]
        wrong_file.write_bytes(before)
        failure(
            "pre-existing-wrong-destination",
            lambda: b.checkout(receiver, A, wrong, "wrong-destination"),
        )
        assert wrong_file.read_bytes() == before
        plan, pin = b.plan(
            A,
            str(sender),
            "s3:release-test/unused",
            [[obj, *content[obj]]],
            "changed-plan",
        )
        plan.write_bytes(plan.read_bytes() + b" ")
        failure("plan-changed-before-execution", lambda: b.execute(plan, pin))
        # Kill the coordinator and its real rclone child while payload transfer is active.
        interrupted = boundary("interruption", "faults/interruption")

        def child_push():
            os.setsid()
            os.environ["HERMES_RUNNER_DEFAULT_BWLIMIT"] = "512K"
            interrupted.push(sender, A, "interrupt")

        context = multiprocessing.get_context("fork")
        child = context.Process(target=child_push)
        child.start()
        rootkey = "releases/" + A
        active = False
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline and child.is_alive():
            descendants = psutil.Process(child.pid).children(recursive=True)
            if any("rclone" in p.name() and "copy" in p.cmdline() for p in descendants):
                active = True
                time.sleep(0.8)
                break
            time.sleep(0.05)
        assert active, "rclone copy did not start"
        os.killpg(child.pid, signal.SIGKILL)
        child.join(timeout=10)
        assert child.exitcode == -signal.SIGKILL and interrupted.maybe(rootkey) is None
        partial = client.list_objects_v2(
            Bucket="release-test", Prefix=interrupted.prefix + "/"
        )
        result["interrupted_remote_before_resume"] = {
            "keys": partial.get("KeyCount", 0),
            "bytes": sum(item["Size"] for item in partial.get("Contents", [])),
            "root_absent": True,
        }
        interrupted_plan = out / "interruption" / "interrupt.plan.json"
        submission = json.loads(
            (out / "interruption" / "interrupt.submission.json").read_bytes()
        )
        raw = interrupted_plan.read_bytes()
        interrupted_plan.write_bytes(raw + b" ")
        failure(
            "plan-changed-before-retry",
            lambda: interrupted.execute(interrupted_plan, submission["blake3"]),
        )
        interrupted_plan.write_bytes(raw)
        timed(
            "interrupted-plan-resume",
            lambda: interrupted.execute(interrupted_plan, submission["blake3"]),
        )
        assert interrupted.maybe(rootkey) is None
        retry = timed(
            "interrupted-publication-retry",
            lambda: interrupted.push(sender, A, "retry"),
        )
        assert retry["release_id"] == A
        result["tests"]["transfer-kill-resume-root-last"] = {
            "pass": True,
            "child_exit": child.exitcode,
            "release_id": A,
        }
        # Race coordinators only after each has checked every target byte.
        race = boundary("race", "faults/race")
        race.repository(create=True)
        for key, info in content.items():
            if not key.startswith("releases/"):
                race.put(key, local_get(sender, key))
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            races = list(
                pool.map(
                    lambda n: Boundary(
                        client, rconfig, out / ("race-" + str(n)), "faults/race"
                    ).push(sender, A, "race"),
                    range(2),
                )
            )
        assert sorted(x["root"] for x in races) == ["created", "equal-existing"]
        assert race.get(rootkey) == local_get(sender, rootkey)
        result["tests"]["two-release-publishers"] = {"pass": True, "results": races}
        record = next(iter(records.values()))
        path = checkout / record[0]
        data = path.read_bytes()
        path.write_bytes(bytes([data[0] ^ 1]) + data[1:])
        failure(
            "checkout-corruption-after-completion",
            lambda: Store(receiver).verify(A, checkout, workers=4),
        )
        # A valid but unsafe root is rejected before materialization.
        badroot = json.loads(local_get(sender, rootkey))
        badroot["descriptor"]["files"].append("../escape")
        raw = canonical(badroot)
        badid = digest(raw)
        b.put("releases/" + badid, raw)
        failure(
            "unsafe-remote-path",
            lambda: b.fetch(badid, work / "unsafe-receiver", "unsafe"),
        )
        # A missing source cannot be satisfied by an old destination file.
        smallsrc = work / "file-only-source"
        smalldst = work / "file-only-dest"
        smallsrc.mkdir()
        smalldst.mkdir()
        (smalldst / "absent").write_bytes(b"old")
        from hermes_core.models import TransferSpec

        strict = b.adapter.run_files(
            TransferSpec(source_path=str(smallsrc), dest_path=str(smalldst)), ["absent"]
        )
        assert not strict.ok
        result["tests"]["missing-source-with-existing-destination"] = {
            "pass": True,
            "outcome": str(strict.outcome),
        }
        server.terminate()
        server.wait(timeout=20)
        command, env = restart
        server = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        for _ in range(100):
            try:
                assert b.get(rootkey) == local_get(sender, rootkey)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise AssertionError("restart failed")
        timed(
            "fetch-after-server-restart",
            lambda: b.fetch(A, work / "restart-receiver", "restart"),
        )
        result["tests"]["server-restart-persisted-data"] = {"pass": True}
        server.terminate()
        server.wait(timeout=20)
        server = None
        sender.rename(hidden)
        source.rename(work / "source-unavailable")
        block["active"] = True
        final = work / "final-offline-checkout"
        timed(
            "offline-checkout",
            lambda: b.checkout(work / "restart-receiver", A, final, "offline"),
        )
        # A separate process opens only the receiver and destination paths.
        code = "from zarrsum.releases import Store; import json,sys; print(json.dumps(Store(sys.argv[1]).verify(sys.argv[2],sys.argv[3],workers=4)))"
        off = subprocess.run(
            [sys.executable, "-c", code, str(work / "restart-receiver"), A, str(final)],
            capture_output=True,
            text=True,
            check=True,
        )
        result["offline_fresh_verify"] = json.loads(off.stdout)
        block["active"] = False
        for release_id, retained in (
            (A, hidden),
            (B, hidden),
            (rp["release_id"], rs.path),
        ):
            _, graph_content, _ = graph(
                release_id, lambda key: local_get(retained, key)
            )
            for key in graph_content:
                if not key.startswith("objects/"):
                    path = out / "graphs" / release_id / key
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(local_get(retained, key))
        result["exact_metadata_graphs_preserved"] = True
        result["pass"] = True
    except BaseException:
        result["pass"] = False
        result["error"] = traceback.format_exc()
        raise
    finally:
        if "block" in locals():
            block["active"] = False
        if server:
            server.terminate()
            server.wait(timeout=20)
        monitor_stop.set()
        thread.join(timeout=2)
        result["boundaries"] = {
            name: {
                "events": b.events,
                "transfers": b.transfers,
                "sdk_requests": b.requests,
            }
            for name, b in boundaries
        }
        result["peaks_sampled_250ms"] = peaks
        result["parent_maxrss_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result["end_utc"] = utc()
        (out / "commands.json").write_text(
            json.dumps(command_log, indent=2, default=str)
        )
        saved()
        # Keep credentials and disposable objects out of the evidence directory.
        shutil.rmtree(work)
        result["temporary_resources_removed"] = not work.exists()
        saved()


def utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


if __name__ == "__main__":
    main()
