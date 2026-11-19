"""Exercise 100,000 exact objects on two compute hosts using installed CLI commands.

Shared storage contains only code and evidence. Payloads and temporary graph indexes
remain node local. A failed push and retry precede independent recovery.
"""

import ctypes
import json
import os
from pathlib import Path
import secrets
import shutil
import socket

import push_scaling as h
from zarrsum.releases import Store, canonical, keys, atomic_file, descriptor
from zarrsum.remotes import graph, local_get

COUNT = 100000


def fixture():
    source = h.WORK / "fixture/source"
    source.mkdir(parents=True)
    store = Store(h.WORK / "fixture/store")
    raw = "".join(f"arrays/a{i:04d}\n" for i in range(1000)).encode()
    registry, count = store.put_registry(raw)
    spec = {
        "version": 2,
        "partitions": 64,
        "files": [],
        "grids": [],
        "families": [
            {
                "registry": registry,
                "count": count,
                "files": ["meta"],
                "grids": [{"prefix": "c", "grid_shape": [99]}],
            }
        ],
    }
    for i, key in enumerate(keys(descriptor(spec, {registry: raw}), {registry: raw})):
        target = source / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(i.to_bytes(8, "little") + bytes([i % 251]) * 1016)
    result = store.publish(
        source, spec, source_id="synthetic-100k-A", assume_immutable=True, workers=8
    )
    assert result["files"] == COUNT and result["bytes"] == COUNT * 1024
    return source, store, spec, result


def sender():
    ctypes.CDLL(None).prctl(36, 1, 0, 0, 0)
    session = {
        "address": socket.gethostbyname(h.HOST),
        "access": secrets.token_hex(10),
        "secret": secrets.token_hex(24),
        "server_host": h.HOST,
        "server_root": str(h.WORK),
    }
    session["endpoint"] = "http://" + session["address"] + ":13900"
    h.SECRET_VALUES.extend([session["access"], session["secret"]])
    log = open(h.WORK / "minio.log", "w")
    client = h.start_minio(session, log)
    client.create_bucket(Bucket="release-test")
    config = h.rconfig_for(session)
    source, store, spec, publication = h.timed("fixture-and-publication", fixture)
    rid = publication["release_id"]
    _, content, records = graph(rid, lambda k: local_get(store.path, k))
    summary = {
        "release_id": rid,
        "files": len(records),
        "bytes": publication["bytes"],
        "graph_keys": len(content),
        "payload_size_bytes": 1024,
        "arrays": 1000,
        "synthetic": True,
        "publication": publication,
    }
    assert summary["files"] == COUNT and len(content) == COUNT + 66
    h.RESULT["fixture"] = summary
    remote = h.boundary(client, config, "large", "graph-100k")
    atomic_file(h.CONTROL / "session.json", canonical(session))
    atomic_file(h.CONTROL / "ready.json", canonical(summary))
    observation, resume = h.interrupted_push(
        client, remote, store.path, rid, content, "term", session, log
    )
    h.RESULT["failed_push"] = observation
    h.save()
    atomic_file(h.CONTROL / "failed.json", canonical({"root_absent": True}))
    h.wait_file(h.CONTROL / "receiver-refused.json", 1200)
    key = observation["complete_payloads"][0]
    original = remote.get(key)
    remote.put(key, bytes([original[0] ^ 1]) + original[1:])
    refused = h.execute_cli(
        remote, "push", store.path, rid, "corrupt-refusal", resume=resume, expected=2
    )
    assert "corrupt remote object" in refused["error"]
    assert h.root_absent(client, remote.prefix, rid)
    client.delete_object(Bucket="release-test", Key=remote.prefix + "/" + key)
    h.RESULT["corrupt_partial_refused"] = True
    receipt = h.execute_cli(remote, "push", store.path, rid, "retry", resume=resume)
    assert receipt["files"] == COUNT and receipt["plan_count"] == 11
    assert receipt["reused_keys"] > 0 and receipt["result"] == "verified"
    h.RESULT["retry"] = receipt
    hidden = h.WORK / "hidden-source"
    source.rename(hidden)
    h.RESULT["source_hidden"] = True
    h.save()
    atomic_file(h.CONTROL / "published.json", canonical({"release_id": rid}))
    h.wait_file(h.CONTROL / "cold-and-warm.json", 3600)
    hidden.rename(source)
    changed = "arrays/a0000/meta"
    (source / changed).write_bytes(b"updated-100k-metadata")
    updated = store.publish(
        source,
        spec,
        parent=rid,
        changed=[changed],
        source_id="synthetic-100k-B",
        assume_immutable=True,
        assume_complete_changes=True,
        workers=8,
    )
    bid = updated["release_id"]
    receipt = h.execute_cli(remote, "push", store.path, bid, "metadata-push")
    h.RESULT["metadata_push"] = receipt
    atomic_file(h.CONTROL / "updated.json", canonical({"release_id": bid}))
    h.wait_file(h.CONTROL / "updated-received.json", 3600)
    h.stop_minio()
    source.parent.rename(h.WORK / "hidden-fixture")
    atomic_file(
        h.CONTROL / "stopped.json",
        canonical({"denied": [str(source), str(store.path)]}),
    )
    h.wait_file(h.CONTROL / "offline-complete.json", 1800)
    h.RESULT["pass"] = True


def receiver():
    summary = h.wait_file(h.CONTROL / "ready.json", 600)
    session = h.wait_file(h.CONTROL / "session.json")
    assert session["server_host"] != h.HOST
    assert not Path(session["server_root"]).exists()
    with socket.create_connection((session["address"], 13900), timeout=5) as connection:
        h.RESULT["network"] = {
            "peer": connection.getpeername(),
            "local": connection.getsockname(),
        }
    client = h.client_for(session)
    config = h.rconfig_for(session)
    remote = h.boundary(client, config, "large", "graph-100k")
    rid = summary["release_id"]
    h.RESULT["fixture"] = summary
    h.wait_file(h.CONTROL / "failed.json", 1800)
    target = h.WORK / "unpublished"
    refusal = h.execute_cli(
        remote,
        "pull",
        h.WORK / "empty",
        rid,
        "unpublished",
        destination=target,
        expected=2,
    )
    assert not target.exists()
    h.RESULT["unpublished_refused"] = refusal
    atomic_file(h.CONTROL / "receiver-refused.json", canonical({"pass": True}))
    h.wait_file(h.CONTROL / "published.json", 3600)
    cache = h.WORK / "cache"
    cold = h.execute_cli(
        remote, "pull", cache, rid, "cold", destination=h.WORK / "checkout"
    )
    assert cold["checkout"]["ok"] and cold["checkout"]["files"] == COUNT
    h.RESULT["cold"] = cold
    shutil.rmtree(h.WORK / "checkout")
    warm = h.execute_cli(
        remote, "pull", cache, rid, "warm", destination=h.WORK / "warm"
    )
    assert warm["fetch"]["downloaded_objects"] == 0 and warm["checkout"]["ok"]
    h.RESULT["warm"] = warm
    shutil.rmtree(h.WORK / "warm")
    h.save()
    atomic_file(h.CONTROL / "cold-and-warm.json", canonical({"pass": True}))
    bid = h.wait_file(h.CONTROL / "updated.json", 3600)["release_id"]
    updated = h.execute_cli(
        remote, "pull", cache, bid, "updated", destination=h.WORK / "updated"
    )
    assert updated["fetch"]["downloaded_objects"] == 1 and updated["checkout"]["ok"]
    h.RESULT["updated"] = updated
    shutil.rmtree(h.WORK / "updated")
    atomic_file(h.CONTROL / "updated-received.json", canonical({"pass": True}))
    stopped = h.wait_file(h.CONTROL / "stopped.json", 1800)
    os.environ["DV_SOURCE_DENY_JSON"] = json.dumps(stopped["denied"])
    offline = h.execute_cli(
        remote, "checkout", cache, rid, "offline", destination=h.WORK / "offline"
    )
    verdict = remote.command(
        ["verify", str(cache), rid, "--destination", str(h.WORK / "offline")]
    )
    assert offline["ok"] and verdict["ok"] and verdict["files"] == COUNT
    h.RESULT["offline"] = {"checkout": offline, "verify": verdict}
    atomic_file(h.CONTROL / "offline-complete.json", canonical({"pass": True}))
    h.RESULT["pass"] = True


if __name__ == "__main__":
    h.sender, h.receiver = sender, receiver
    h.main()
