"""Publish exact file sets from computed paths. The caller supplies an immutable source.

This local POC has one publisher and retains all releases. It does not discover files,
provide snapshots, or remove stored objects. Its identity is separate from zarrsum-id/1.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import itertools
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tempfile
import time

from blake3 import blake3

FORMAT = "computed-release/1"
BLOCK = 4 << 20
PARTITIONS = 64
MAX_PARTITION_FILES = 100_000


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(data):
    return blake3(data).hexdigest()


def member(value):
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("invalid member path")
    path = PurePosixPath(value)
    if path.is_absolute() or str(path) != value or any(p in (".", "..") for p in value.split("/")):
        raise ValueError(f"unsafe member path: {value!r}")
    return value


def identifier(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("expected a 64-character lowercase digest")
    return value


def descriptor(value):
    """Normalize a finite key grammar and reject overlapping file namespaces."""
    if not isinstance(value, dict) or set(value) != {"version", "files", "grids"} or value["version"] != 1:
        raise ValueError("descriptor needs version=1, files, and grids")
    if not isinstance(value["files"], list) or not isinstance(value["grids"], list):
        raise ValueError("files and grids must be lists")
    files = sorted(member(p) for p in value["files"])
    if len(set(files)) != len(files):
        raise ValueError("duplicate explicit file")
    grids = []
    for item in value["grids"]:
        if not isinstance(item, dict) or set(item) - {"prefix", "grid_shape", "separator", "present"}:
            raise ValueError("invalid grid fields")
        prefix = member(item["prefix"])
        shape = item["grid_shape"]
        separator = item.get("separator", "/")
        if not isinstance(shape, list) or not shape or any(type(n) is not int or n < 1 for n in shape):
            raise ValueError("grid_shape needs positive integers")
        if separator not in ("/", "."):
            raise ValueError("separator must be / or .")
        grid = {"prefix": prefix, "grid_shape": shape, "separator": separator}
        if "present" in item:
            present = item["present"]
            if not isinstance(present, list):
                raise ValueError("present must be a coordinate list")
            coords = []
            for coordinate in present:
                if not isinstance(coordinate, list) or len(coordinate) != len(shape) or any(
                    type(n) is not int or not 0 <= n < size for n, size in zip(coordinate, shape)
                ):
                    raise ValueError("invalid sparse coordinate")
                coords.append(tuple(coordinate))
            if len(set(coords)) != len(coords):
                raise ValueError("duplicate sparse coordinate")
            grid["present"] = sorted(coords)
        grids.append(grid)
    grids.sort(key=lambda g: g["prefix"])
    prefixes = [g["prefix"] for g in grids]
    if len(set(prefixes)) != len(prefixes):
        raise ValueError("duplicate grid prefix")
    for namespaces in (files, prefixes):
        namespace_set = set(namespaces)
        for path in namespaces:
            parts = path.split("/")
            if any("/".join(parts[:i]) in namespace_set for i in range(1, len(parts))):
                raise ValueError(f"overlapping namespace: {path}")
    for path in files:
        for grid in grids:
            prefix = grid["prefix"]
            if prefix == path or prefix.startswith(path + "/"):
                raise ValueError(f"file overlaps grid prefix: {path}")
            if not path.startswith(prefix + "/"):
                continue
            suffix = path[len(prefix) + 1:]
            shape = grid["grid_shape"]
            if grid["separator"] == "/":
                coordinates = suffix.split("/")[:len(shape)]
            else:
                coordinates = suffix.split("/")[0].split(".")
                if len(coordinates) != len(shape):
                    continue
            if all(p.isascii() and p.isdigit() and str(int(p)) == p and int(p) < n
                   for p, n in zip(coordinates, shape)):
                raise ValueError(f"file overlaps generated key: {path}")
    if not files and not any("present" not in g or g["present"] for g in grids):
        raise ValueError("empty releases are not supported")
    return {"version": 1, "files": files, "grids": grids}


def keys(spec):
    yield from spec["files"]
    for grid in spec["grids"]:
        coordinates = grid.get("present")
        if coordinates is None:
            coordinates = dense_coordinates(grid["grid_shape"])
        for coordinate in coordinates:
            yield grid["prefix"] + "/" + grid["separator"].join(map(str, coordinate))


def dense_coordinates(shape):
    coordinate = [0] * len(shape)
    while True:
        yield tuple(coordinate)
        for axis in range(len(shape) - 1, -1, -1):
            coordinate[axis] += 1
            if coordinate[axis] < shape[axis]:
                break
            coordinate[axis] = 0
        else:
            return


@contextmanager
def open_member(root, key):
    """Open a regular member without following member symlinks."""
    parts = member(key).split("/")
    parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    file_fd = None
    try:
        for part in parts[:-1]:
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = next_fd
        file_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        if not stat.S_ISREG(os.fstat(file_fd).st_mode):
            raise ValueError(f"not a regular file: {key}")
        with os.fdopen(file_fd, "rb") as stream:
            file_fd = None
            yield stream
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(parent)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_file(path, data):
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class Store:
    """A local retained store. The store directory must be owned by the caller."""

    def __init__(self, path):
        self.path = Path(path).resolve()
        for name in ("objects", "manifests", "releases", "checkpoints", "tmp"):
            (self.path / name).mkdir(parents=True, exist_ok=True)

    def object_path(self, value):
        return self.path / "objects" / identifier(value)

    def _install(self, temporary, target, expected):
        try:
            os.link(temporary, target)
        except FileExistsError:
            actual = blake3()
            with open(target, "rb") as stream:
                while block := stream.read(BLOCK):
                    actual.update(block)
            if actual.hexdigest() != expected:
                raise ValueError(f"corrupt retained object: {target}")

    def _put_manifest(self, data, directory="manifests"):
        value = digest(data)
        target = self.path / directory / value
        fd, temporary = tempfile.mkstemp(dir=self.path / "tmp")
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            self._install(temporary, target, value)
            sync_directory(target.parent)
        finally:
            os.unlink(temporary)
        return value

    def _load(self, directory, value):
        data = (self.path / directory / identifier(value)).read_bytes()
        if digest(data) != value:
            raise ValueError(f"corrupt {directory}: {value}")
        return json.loads(data)

    def _retain(self, source, key):
        hasher = blake3()
        size = 0
        fd, temporary = tempfile.mkstemp(dir=self.path / "tmp")
        try:
            with os.fdopen(fd, "wb") as output, open_member(source, key) as stream:
                before = os.fstat(stream.fileno())
                while block := stream.read(BLOCK):
                    hasher.update(block)
                    output.write(block)
                    size += len(block)
                after = os.fstat(stream.fileno())
                signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                if signature(before) != signature(after) or size != before.st_size:
                    raise ValueError(f"source changed while reading: {key}")
                output.flush()
                os.fsync(output.fileno())
            value = hasher.hexdigest()
            self._install(temporary, self.object_path(value), value)
            return [key, size, value]
        finally:
            os.unlink(temporary)

    def _check_record(self, record, output=None):
        key, expected_size, value = record
        member(key)
        identifier(value)
        if type(expected_size) is not int or expected_size < 0:
            raise ValueError("invalid object length")
        hasher = blake3()
        size = 0
        with open(self.object_path(value), "rb") as stream:
            while block := stream.read(BLOCK):
                hasher.update(block)
                size += len(block)
                if output is not None:
                    output.write(block)
        if size != expected_size or hasher.hexdigest() != value:
            raise ValueError(f"retained bytes mismatch: {key}")
        return size

    def publish(self, source, spec, *, source_id, assume_immutable=False, workers=8,
                resume=True, parent=None, changed=None, assume_complete_changes=False,
                progress=None):
        """Publish from a caller-guaranteed immutable source; reuse completed partitions.

        source_id must name that fixed source version, not a mutable dataset name. The
        local POC cannot enforce this contract. progress is called after durable checkpoints.
        """
        if not assume_immutable or not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("publication requires an immutable source assertion and source_id")
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be positive")
        spec = descriptor(spec)
        previous = None
        changed_keys = set()
        if parent is not None:
            if not assume_complete_changes or not isinstance(changed, list):
                raise ValueError("parent reuse requires an asserted complete changed-key list")
            changed_keys = {member(key) for key in changed}
            if len(changed_keys) != len(changed):
                raise ValueError("duplicate changed key")
            previous = self._load("releases", parent)
            if previous["format"] != FORMAT or canonical(previous["descriptor"]) != canonical(spec):
                raise ValueError("delta publication requires the same membership descriptor")
        elif changed is not None or assume_complete_changes:
            raise ValueError("change records require a parent release")
        source = Path(source).resolve(strict=True)
        if not source.is_dir():
            raise ValueError("source must be a directory")
        if self.path == source or self.path.is_relative_to(source) or source.is_relative_to(self.path):
            raise ValueError("source and store must be separate trees")
        request = {"format": FORMAT, "descriptor": spec, "source": str(source),
                   "source_id": source_id, "parent": parent, "changed": sorted(changed_keys)}
        checkpoint_key = digest(canonical(request))
        checkpoint = self.path / "checkpoints" / (checkpoint_key + ".json")
        started = time.monotonic()
        with open(self.path / "publisher.lock", "a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            state = {"request": request, "parts": {}}
            if resume and checkpoint.exists():
                state = json.loads(checkpoint.read_bytes())
                if canonical(state["request"]) != canonical(request):
                    raise ValueError("checkpoint does not match source")
            with tempfile.TemporaryDirectory(dir=self.path / "tmp", prefix="plans-") as plans:
                plan_paths = [Path(plans) / str(i) for i in range(PARTITIONS)]
                handles = [open(p, "wb") for p in plan_paths]
                counts = [0] * PARTITIONS
                unmatched = set(changed_keys)
                try:
                    for key in keys(spec):
                        partition = int(digest(key.encode())[:8], 16) % PARTITIONS
                        handles[partition].write(canonical(key) + b"\n")
                        counts[partition] += 1
                        unmatched.discard(key)
                        if counts[partition] > MAX_PARTITION_FILES:
                            raise ValueError("partition exceeds the POC file-count limit")
                finally:
                    for handle in handles:
                        handle.close()
                if unmatched:
                    raise ValueError(f"changed keys are outside the descriptor: {sorted(unmatched)}")
                previous_parts = {p: (value, count, size) for p, value, count, size in
                                  previous["partitions"]} if previous else {}
                changed_parts = {int(digest(key.encode())[:8], 16) % PARTITIONS for key in changed_keys}

                def process(partition):
                    prior = state["parts"].get(str(partition))
                    if prior:
                        records = self._load("manifests", prior)
                        with open(plan_paths[partition], "rb") as plan:
                            expected = (json.loads(line) for line in plan)
                            sentinel = object()
                            for record, key in itertools.zip_longest(records, expected, fillvalue=sentinel):
                                if record is sentinel or key is sentinel or record[0] != key:
                                    raise ValueError("checkpoint membership mismatch")
                                self._check_record(record)
                        return partition, prior, len(records), sum(r[1] for r in records), True, 0, 0
                    if previous and partition not in changed_parts:
                        value, count, size = previous_parts[partition]
                        self._load("manifests", value)
                        return partition, value, count, size, True, 0, 0
                    old_records = {}
                    if previous:
                        old_records = {r[0]: r for r in self._load("manifests", previous_parts[partition][0])}
                    records = []
                    source_count = source_bytes = 0
                    with open(plan_paths[partition], "rb") as plan:
                        for line in plan:
                            key = json.loads(line)
                            if previous and key not in changed_keys:
                                record = old_records[key]
                            else:
                                record = self._retain(source, key)
                                source_count += 1
                                source_bytes += record[1]
                            records.append(record)
                    value = self._put_manifest(canonical(records))
                    return partition, value, len(records), sum(r[1] for r in records), False, source_count, source_bytes

                active = [i for i, count in enumerate(counts) if count]
                results = []
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {i: executor.submit(process, i) for i in active}
                    for partition in active:
                        result = futures[partition].result()
                        results.append(result)
                        state["parts"][str(partition)] = result[1]
                        sync_directory(self.path / "objects")
                        atomic_file(checkpoint, canonical(state))
                        if progress:
                            progress(len(results))
                root = {"format": FORMAT, "descriptor": spec,
                        "partitions": [list(r[:4]) for r in results]}
                release_id = self._put_manifest(canonical(root), "releases")
        return {"release_id": release_id, "files": sum(r[2] for r in results),
                "bytes": sum(r[3] for r in results), "partitions": len(results),
                "reused_partitions": sum(r[4] for r in results),
                "source_files_read": sum(r[5] for r in results),
                "source_bytes_read": sum(r[6] for r in results),
                "wall_s": time.monotonic() - started, "retention": "owned-copy",
                "source_consistency": "caller-asserted-immutable"}

    def records(self, release_id):
        root = self._load("releases", release_id)
        if root["format"] != FORMAT:
            raise ValueError("unsupported release format")
        for _partition, value, count, size in root["partitions"]:
            records = self._load("manifests", value)
            if len(records) != count or sum(r[1] for r in records) != size:
                raise ValueError("partition totals mismatch")
            yield from records

    def verify(self, release_id, destination=None):
        """Read retained bytes, or read the declared files in a restored directory."""
        files = size = 0
        for record in self.records(release_id):
            key, expected_size, value = record
            if destination is None:
                size += self._check_record(record)
            else:
                hasher = blake3()
                count = 0
                with open_member(destination, key) as stream:
                    while block := stream.read(BLOCK):
                        hasher.update(block)
                        count += len(block)
                if count != expected_size or hasher.hexdigest() != value:
                    raise ValueError(f"destination bytes mismatch: {key}")
                size += count
            files += 1
        return {"ok": True, "release_id": release_id, "files": files, "bytes": size,
                "scope": "declared-members-only", "check": "fresh-bytes"}

    def restore(self, release_id, destination):
        """Restore to a new directory; check each retained object during the copy."""
        destination = Path(destination).absolute()
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(destination)
        if destination == self.path or destination.is_relative_to(self.path):
            raise ValueError("restore destination must be outside the store")
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(dir=destination.parent, prefix=".restore-"))
        try:
            for record in self.records(release_id):
                output = staging / member(record[0])
                output.parent.mkdir(parents=True, exist_ok=True)
                with open(output, "xb") as stream:
                    self._check_record(record, stream)
            if destination.exists() or destination.is_symlink():
                raise FileExistsError(destination)
            os.rename(staging, destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
        return {"release_id": release_id, "destination": str(destination), "ok": True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("store")
    publish.add_argument("source")
    publish.add_argument("descriptor")
    publish.add_argument("--source-id", required=True)
    publish.add_argument("--assume-immutable-source", action="store_true")
    publish.add_argument("--workers", type=int, default=8)
    publish.add_argument("--no-resume", action="store_true")
    publish.add_argument("--parent")
    publish.add_argument("--changes", help="JSON list of all changed logical keys")
    publish.add_argument("--assume-complete-changes", action="store_true")
    verify = commands.add_parser("verify")
    verify.add_argument("store")
    verify.add_argument("release_id")
    verify.add_argument("--destination")
    restore = commands.add_parser("restore")
    restore.add_argument("store")
    restore.add_argument("release_id")
    restore.add_argument("destination")
    args = parser.parse_args(argv)
    try:
        store = Store(args.store)
        if args.command == "publish":
            with open(args.descriptor, "rb") as stream:
                spec = json.load(stream)
            changed = None
            if args.changes:
                with open(args.changes, "rb") as stream:
                    changed = json.load(stream)
            result = store.publish(args.source, spec, source_id=args.source_id,
                                   assume_immutable=args.assume_immutable_source,
                                   workers=args.workers, resume=not args.no_resume,
                                   parent=args.parent, changed=changed,
                                   assume_complete_changes=args.assume_complete_changes)
        elif args.command == "verify":
            result = store.verify(args.release_id, args.destination)
        else:
            result = store.restore(args.release_id, args.destination)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(2, f"computed-release: {error}\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
