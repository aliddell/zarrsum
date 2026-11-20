"""Keep metadata revisions separate from cooperative immutable payload storage.

This local experiment trusts one publisher and one retained filesystem. It is
separate from computed-release/1. Reused payloads receive stat checks, not fresh
content verification. File permissions do not constrain the storage owner.
"""

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import uuid
import tempfile
import time

from .releases import canonical, digest, identifier, member

FORMAT = "metadata-release/experimental-1"
MAX_FILES = 10_000
MAX_METADATA = 1 << 20


def write_json(path, value):
    path = Path(path)
    with path.open("xb") as stream:
        stream.write(canonical(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def stamp(path):
    info = Path(path).lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o222:
        raise ValueError("The retained object must be a read-only regular file")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]


def metadata(data):
    if not isinstance(data, bytes) or len(data) > MAX_METADATA:
        raise ValueError("Metadata exceeds the byte bound")
    return data


def nfs_pool_identity(root):
    root = Path(root).resolve(strict=True)
    fields = subprocess.check_output(
        ["findmnt", "-T", str(root), "-n", "-o", "FSTYPE,SOURCE"],
        text=True, timeout=30,
    ).strip().split(maxsplit=1)
    if len(fields) != 2 or fields[0] not in {"nfs", "nfs4"}:
        raise ValueError("Explicit shared-pool mode requires an NFS mount")
    export = fields[1].rpartition(":")[2]
    if not export.startswith("/"):
        raise ValueError("Cannot establish the NFS export path")
    return {"fstype": fields[0], "export_path": export, "root": str(root),
            "pool_id": uuid.uuid4().hex}


class Repository:
    def __init__(self, root, *, shared_filesystem=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("objects", "manifests", "receipts"):
            (self.root / name).mkdir(exist_ok=True)
        self.shared_filesystem = shared_filesystem
        self._local_device = self.root.stat().st_dev
        marker = self.root / "shared-filesystem.json"
        if marker.exists() and shared_filesystem is None:
            raise ValueError("Shared-pool verification must be selected explicitly")
        if shared_filesystem is not None:
            actual = nfs_pool_identity(self.root)
            if (not isinstance(shared_filesystem, dict)
                    or set(shared_filesystem) != set(actual)
                    or any(actual[key] != shared_filesystem[key] for key in ["fstype", "export_path", "root"])
                    or not isinstance(shared_filesystem.get("pool_id"), str)
                    or len(shared_filesystem["pool_id"]) != 32):
                raise ValueError("Shared filesystem identity differs")
            if marker.exists():
                if json.loads(marker.read_bytes()) != shared_filesystem:
                    raise ValueError("Shared-pool marker differs")
            else:
                if any(any((self.root/name).iterdir()) for name in ("objects", "manifests", "receipts")):
                    raise ValueError("Cannot reinterpret legacy retained-object receipts")
                write_json(marker, shared_filesystem)
                marker.chmod(0o444)
        with self.database() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS releases (
                    id TEXT PRIMARY KEY, dataset TEXT NOT NULL, revision TEXT NOT NULL,
                    UNIQUE(dataset, revision));
                CREATE TABLE IF NOT EXISTS replicas (
                    id TEXT NOT NULL REFERENCES releases(id), site TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE, receipt TEXT NOT NULL,
                    PRIMARY KEY(id, site));
                CREATE TABLE IF NOT EXISTS ready (
                    id TEXT NOT NULL, site TEXT NOT NULL, validation TEXT NOT NULL,
                    PRIMARY KEY(id, site),
                    FOREIGN KEY(id, site) REFERENCES replicas(id, site));
                CREATE TABLE IF NOT EXISTS recommended (
                    dataset TEXT PRIMARY KEY, id TEXT NOT NULL REFERENCES releases(id));
            """)

    def _stamp(self, path):
        value = stamp(path)
        if self.shared_filesystem is not None:
            if value[0] != self._local_device:
                raise ValueError("Retained object left the selected shared filesystem")
            value[0] = json.dumps(self.shared_filesystem, sort_keys=True, separators=(",", ":"))
        return value

    @contextmanager
    def database(self):
        db = sqlite3.connect(self.root / "catalog.sqlite", timeout=30)
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def lock(self):
        with (self.root / "publisher.lock").open("a") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            yield

    def object_path(self, value):
        return self.root / "objects" / identifier(value)

    def manifest(self, release_id):
        raw = (self.root / "manifests" / identifier(release_id)).read_bytes()
        if digest(raw) != release_id:
            raise ValueError("Manifest digest mismatch")
        value = json.loads(raw)
        if value["format"] != FORMAT:
            raise ValueError("Unexpected release format")
        return value

    def _retain(self, source):
        temporary = self.root / "objects" / (".incoming-" + str(time.time_ns()))
        from blake3 import blake3

        hashed = blake3()
        try:
            with Path(source).open("rb") as src, temporary.open("xb") as dst:
                before = os.fstat(src.fileno())
                if not stat.S_ISREG(before.st_mode):
                    raise ValueError("Source must be a regular file")
                while block := src.read(4 << 20):
                    hashed.update(block)
                    dst.write(block)
                after = os.fstat(src.fileno())
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_size, after.st_mtime_ns, after.st_ctime_ns
                ):
                    raise ValueError("Source changed during retention")
                dst.flush()
                os.fsync(dst.fileno())
            value = hashed.hexdigest()
            target = self.object_path(value)
            temporary.chmod(0o444)
            if target.exists():
                if self._stamp(target)[2] != after.st_size:
                    raise ValueError("Retained size mismatch")
                check = blake3()
                with target.open("rb") as stream:
                    while block := stream.read(4 << 20):
                        check.update(block)
                if check.hexdigest() != value:
                    raise ValueError("Existing retained content mismatch")
                temporary.unlink()
            else:
                temporary.rename(target)
            return value, self._stamp(target), after.st_size
        finally:
            temporary.unlink(missing_ok=True)

    def _save(self, document):
        raw = canonical(document)
        release_id = digest(raw)
        target = self.root / "manifests" / release_id
        with self.database() as db:
            previous = db.execute(
                "SELECT id FROM releases WHERE dataset=? AND revision=?",
                (document["dataset"], document["revision"]),
            ).fetchone()
            if previous:
                raise ValueError("A dataset revision cannot be reused")
            if not target.exists():
                with target.open("xb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                target.chmod(0o444)
            db.execute("INSERT INTO releases VALUES (?, ?, ?)",
                       (release_id, document["dataset"], document["revision"]))
        return release_id

    def publish(self, dataset, revision, source, paths, metadata_paths):
        paths = [member(path) for path in paths]
        meta = set(metadata_paths)
        if (not dataset or not revision or not paths or len(paths) > MAX_FILES
                or len(paths) != len(set(paths)) or not meta <= set(paths)):
            raise ValueError("Invalid release membership")
        source = Path(source).resolve(strict=True)
        with self.lock():
            entries = []
            payload_bytes = 0
            for path in sorted(paths):
                original = source / path
                if original.is_symlink() or not original.resolve(strict=True).is_relative_to(source):
                    raise ValueError("Source path escapes the declared root")
                if path in meta and original.stat().st_size > MAX_METADATA:
                    raise ValueError("Metadata exceeds the byte bound")
                value, identity, size = self._retain(original)
                kind = "metadata" if path in meta else "payload"
                entries.append(dict(path=path, kind=kind, size=size, blake3=value))
                if kind == "payload":
                    payload_bytes += size
                write_json(self.root / "receipts" / (value + "-" + str(time.time_ns())),
                           dict(blake3=value, stat=identity, source_read="full-bytes"))
            result = self._save(dict(format=FORMAT, dataset=dataset, revision=revision,
                                     parent=None, entries=entries))
            return result, dict(payload_bytes_read=payload_bytes, files=len(entries))

    def _check_objects(self, document):
        for entry in document["entries"]:
            target = self.object_path(entry["blake3"])
            observed = self._stamp(target)
            if observed[2] != entry["size"]:
                raise ValueError("Retained size mismatch")
            receipts = list((self.root / "receipts").glob(entry["blake3"] + "-*"))
            if not receipts or not any(json.loads(p.read_bytes())["stat"] == observed for p in receipts):
                raise ValueError("Retained object changed since registration")
            if entry["kind"] == "metadata" and digest(target.read_bytes()) != entry["blake3"]:
                raise ValueError("Metadata content mismatch")

    def update(self, parent_id, revision, replacements):
        if not replacements:
            raise ValueError("Specify a metadata change")
        with self.lock():
            parent = self.manifest(parent_id)
            self._check_objects(parent)
            by_path = {entry["path"]: dict(entry) for entry in parent["entries"]}
            for path, data in replacements.items():
                member(path)
                if path not in by_path or by_path[path]["kind"] != "metadata":
                    raise ValueError("Update can replace only declared metadata")
                metadata(data)
                if Path(path).name == "zarr.json":
                    before = json.loads(self.object_path(by_path[path]["blake3"]).read_bytes())
                    after = json.loads(data)
                    if not isinstance(after, dict):
                        raise ValueError("Zarr metadata must be an object")
                    before.pop("attributes", None)
                    structure = {key: value for key, value in after.items() if key != "attributes"}
                    if before != structure:
                        raise ValueError("Array layout changes require a new ingest")
                temporary = self.root / (".metadata-" + str(time.time_ns()))
                try:
                    temporary.write_bytes(data)
                    value, identity, size = self._retain(temporary)
                finally:
                    temporary.unlink(missing_ok=True)
                write_json(self.root / "receipts" / (value + "-" + str(time.time_ns())),
                           dict(blake3=value, stat=identity, source_read="full-metadata-bytes"))
                by_path[path].update(size=size, blake3=value)
            result = self._save(dict(format=FORMAT, dataset=parent["dataset"],
                                     revision=revision, parent=parent_id,
                                     entries=[by_path[key] for key in sorted(by_path)]))
            return result, dict(payload_bytes_read=0, identity_basis="inherited-full-digests",
                                reuse_check="read-only-object-stat", publisher_trust=True)

    def stage(self, release_id, site, destination, after_member=None):
        destination = Path(destination).absolute()
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with self.lock():
            document = self.manifest(release_id)
            self._check_objects(document)
            temporary = Path(tempfile.mkdtemp(prefix=".staging-", dir=destination.parent))
            try:
                for entry in document["entries"]:
                    source = self.object_path(entry["blake3"])
                    target = temporary / entry["path"]
                    target.parent.mkdir(parents=True, exist_ok=True)
                    if entry["kind"] == "payload":
                        os.link(source, target)
                    else:
                        shutil.copyfile(source, target)
                        target.chmod(0o444)
                    if after_member:
                        after_member(entry["path"])
                receipt = dict(release_id=release_id, site=site, path=str(destination),
                               identity_basis="inherited-full-digests",
                               payload_verification="read-only-object-stat",
                               payload_bytes_read=0, payload_bytes_copied=0,
                               metadata_verification="full-blake3", publisher_trust=True,
                               storage_identity=self.shared_filesystem)
                write_json(temporary / ".release-receipt.json", receipt)
                os.rename(temporary, destination)
                with self.database() as db:
                    db.execute("INSERT INTO replicas VALUES (?, ?, ?, ?)",
                               (release_id, site, str(destination), canonical(receipt).decode()))
                return receipt
            finally:
                if temporary.exists():
                    shutil.rmtree(temporary)

    def check_checkout(self, release_id, destination):
        document = self.manifest(release_id)
        self._check_objects(document)
        destination = Path(destination)
        for entry in document["entries"]:
            target = destination / entry["path"]
            if entry["kind"] == "metadata":
                if target.is_symlink() or digest(target.read_bytes()) != entry["blake3"]:
                    raise ValueError("Checkout metadata mismatch")
            elif self._stamp(target) != self._stamp(self.object_path(entry["blake3"])):
                raise ValueError("Checkout payload identity mismatch")

    def promote(self, release_id, site, validation):
        with self.lock():
            with self.database() as db:
                row = db.execute("SELECT path FROM replicas WHERE id=? AND site=?",
                                 (release_id, site)).fetchone()
                if not row:
                    raise ValueError("Release is not staged")
                if (validation.get("release_id") != release_id
                        or validation.get("result") != "passed"
                        or validation.get("input_root") != row[0]):
                    raise ValueError("A matching loader validation is required")
                self.check_checkout(release_id, row[0])
                db.execute("INSERT INTO ready VALUES (?, ?, ?)",
                           (release_id, site, canonical(validation).decode()))

    def bind(self, dataset, release_id, site):
        identifier(release_id)
        with self.database() as db:
            row = db.execute("""SELECT replicas.path, replicas.receipt FROM releases
                JOIN replicas ON releases.id=replicas.id
                JOIN ready ON ready.id=replicas.id AND ready.site=replicas.site
                WHERE releases.dataset=? AND releases.id=? AND replicas.site=?""",
                             (dataset, release_id, site)).fetchone()
        if not row:
            raise ValueError("The requested exact release is not ready at this site")
        self.check_checkout(release_id, row[0])
        return dict(release_id=release_id, dataset=dataset, site=site, input_root=row[0],
                    preparation=json.loads(row[1]), bound_time_ns=time.time_ns())

    def recommend(self, dataset, release_id, site):
        self.bind(dataset, release_id, site)
        with self.database() as db:
            db.execute("INSERT INTO recommended VALUES (?, ?) ON CONFLICT(dataset) "
                       "DO UPDATE SET id=excluded.id", (dataset, release_id))

    def recommendation(self, dataset):
        with self.database() as db:
            row = db.execute("SELECT id FROM recommended WHERE dataset=?", (dataset,)).fetchone()
        return None if row is None else row[0]
