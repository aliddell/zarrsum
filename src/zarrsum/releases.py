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
MAX_PARTITIONS = 65_536
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


def ancestors(path):
    parts = path.split("/")
    return ("/".join(parts[:i]) for i in range(1, len(parts)))


def _grid(item):
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
    return grid


def _generated(grid, path):
    """Return whether path equals or lies under a key that the grid can generate."""
    suffix = path[len(grid["prefix"]) + 1:]
    shape = grid["grid_shape"]
    if grid["separator"] == "/":
        coordinates = suffix.split("/")[:len(shape)]
    else:
        coordinates = suffix.split("/")[0].split(".")
        if len(coordinates) != len(shape):
            return False
    return all(p.isascii() and p.isdigit() and str(int(p)) == p and int(p) < n
               for p, n in zip(coordinates, shape))


def _namespace(files, grids):
    """Normalize explicit files and grids; reject overlaps in O(members x path depth).

    A prefix index replaces the all-pairs file and grid comparison of the first POC.
    Returns the normalized files, grids, and the set of grid prefixes.
    """
    files = sorted(member(p) for p in files)
    if len(set(files)) != len(files):
        raise ValueError("duplicate explicit file")
    grids = sorted((_grid(g) for g in grids), key=lambda g: g["prefix"])
    prefixes = {g["prefix"]: g for g in grids}
    if len(prefixes) != len(grids):
        raise ValueError("duplicate grid prefix")
    for namespace in (set(files), prefixes):
        for path in namespace:
            if any(a in namespace for a in ancestors(path)):
                raise ValueError(f"overlapping namespace: {path}")
    above = {a for prefix in prefixes for a in ancestors(prefix)}
    for path in files:
        if path in prefixes or path in above:
            raise ValueError(f"file overlaps grid prefix: {path}")
        for a in ancestors(path):
            if a in prefixes and _generated(prefixes[a], path):
                raise ValueError(f"file overlaps generated key: {path}")
    return files, grids, prefixes


def _has_keys(files, grids):
    return bool(files) or any("present" not in g or g["present"] for g in grids)


def iter_registry(data):
    """Validate sorted registry members while retaining one decoded line at a time."""
    import io

    if not data.endswith(b"\n"):
        raise ValueError("registry needs a final newline")
    previous = None
    for line in io.BytesIO(data):
        value = line[:-1].decode("utf-8")
        member(value)
        if previous is not None and not previous < value:
            raise ValueError("registry must be sorted and unique")
        previous = value
        yield value


def read_registry(data):
    return list(iter_registry(data))


def descriptor(value, registries=None, *, index=None):
    """Normalize a finite key grammar and reject overlapping file namespaces.

    Version 1 is the first POC grammar with 64 partitions. Version 2 records the partition
    count and adds families: a template of files and grids repeated under each id of a
    content-addressed registry. The descriptor holds the registry digest and count, not the
    ids. registries maps a digest to the registry bytes; it is needed only to validate v2.
    """
    if not isinstance(value, dict) or value.get("version") not in (1, 2):
        raise ValueError("descriptor needs version=1, files, and grids")
    version = value["version"]
    allowed = {"version", "files", "grids"} | ({"partitions", "families"} if version == 2 else set())
    if set(value) - allowed or not {"files", "grids"} <= set(value):
        raise ValueError("descriptor needs version=1, files, and grids")
    if not isinstance(value["files"], list) or not isinstance(value["grids"], list):
        raise ValueError("files and grids must be lists")
    files, grids, prefixes = _namespace(value["files"], value["grids"])
    if version == 1:
        if not _has_keys(files, grids):
            raise ValueError("empty releases are not supported")
        return {"version": 1, "files": files, "grids": grids}
    partitions = value.get("partitions", PARTITIONS)
    if type(partitions) is not int or not 1 <= partitions <= MAX_PARTITIONS:
        raise ValueError("partitions must be an integer from 1 to 65536")
    families = value.get("families", [])
    if not isinstance(families, list):
        raise ValueError("families must be a list")
    normalized = []
    ids = set() if index is None else index()
    nonempty = _has_keys(files, grids)
    for family in families:
        if not isinstance(family, dict) or set(family) != {"registry", "count", "files", "grids"}:
            raise ValueError("family needs registry, count, files, and grids")
        if not isinstance(family["files"], list) or not isinstance(family["grids"], list):
            raise ValueError("files and grids must be lists")
        registry = identifier(family["registry"])
        t_files, t_grids, _ = _namespace(family["files"], family["grids"])
        if not _has_keys(t_files, t_grids):
            raise ValueError("family template generates no keys")
        if registries is None or registry not in registries:
            raise ValueError(f"missing registry: {registry}")
        data = registries[registry]
        if digest(data) != registry:
            raise ValueError(f"corrupt registry: {registry}")
        count = 0
        for value in iter_registry(data):
            if value in ids:
                raise ValueError("an id appears in two families")
            ids.add(value)
            count += 1
        if type(family["count"]) is not int or family["count"] != count:
            raise ValueError("family count does not match registry")
        nonempty = True
        normalized.append({"registry": registry, "count": count,
                           "files": t_files, "grids": t_grids})
    if len({f["registry"] for f in normalized}) != len(normalized):
        raise ValueError("duplicate family registry")
    if ids:
        id_above = set() if index is None else index()
        for path in ids:
            for a in ancestors(path):
                if a in ids:
                    raise ValueError(f"overlapping registry ids: {path}")
                if a in prefixes:
                    raise ValueError(f"registry id inside grid prefix: {path}")
                id_above.add(a)
        for path in itertools.chain(files, prefixes):
            if path in ids or path in id_above or any(a in ids for a in ancestors(path)):
                raise ValueError(f"explicit member overlaps registry id: {path}")
    if not nonempty:
        raise ValueError("empty releases are not supported")
    normalized.sort(key=lambda f: f["registry"])
    return {"version": 2, "partitions": partitions, "files": files, "grids": grids,
            "families": normalized}


def partition_count(spec):
    return spec.get("partitions", PARTITIONS)


def partition_of(key, count):
    return int(digest(key.encode())[:8], 16) % count


def _grid_keys(grids, base=""):
    for grid in grids:
        coordinates = grid.get("present")
        if coordinates is None:
            coordinates = dense_coordinates(grid["grid_shape"])
        start = base + grid["prefix"] + "/"
        separator = grid["separator"]
        for coordinate in coordinates:
            yield start + separator.join(map(str, coordinate))


def keys(spec, registries=None):
    """Generate every member key in a deterministic order, streaming registry ids."""
    yield from spec["files"]
    yield from _grid_keys(spec["grids"])
    for family in spec.get("families", ()):
        template = list(family["files"])
        template_grids = family["grids"]
        for value in iter_registry(registries[family["registry"]]):
            base = value + "/"
            for name in template:
                yield base + name
            yield from _grid_keys(template_grids, base)


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
        """Open or create a store. New stores fan objects out as objects/ab/cd/<digest>.

        One flat directory serializes creates and links on NFS (Reef profile, 2026-11-16).
        A store without a layout file keeps the flat layout of the first POC.
        """
        self.path = Path(path).resolve()
        created = not (self.path / "objects").exists()
        for name in ("objects", "manifests", "releases", "checkpoints", "registries", "tmp"):
            (self.path / name).mkdir(parents=True, exist_ok=True)
        layout = self.path / "layout"
        if created:
            atomic_file(layout, b"fanout-2\n")
        self.fanout = layout.exists() and layout.read_bytes() == b"fanout-2\n"

    def object_path(self, value):
        value = identifier(value)
        if self.fanout:
            return self.path / "objects" / value[:2] / value[2:4] / value
        return self.path / "objects" / value

    def _temporary(self):
        if not self.fanout:
            return self.path / "tmp"
        directory = self.path / "tmp" / os.urandom(1).hex()
        directory.mkdir(exist_ok=True)
        return directory

    def _install(self, temporary, target, expected):
        if self.fanout:
            target.parent.mkdir(parents=True, exist_ok=True)
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

    def put_registry(self, data):
        """Retain registry bytes by content digest and return (digest, id count)."""
        count = len(read_registry(data))
        return self._put_manifest(data, "registries"), count

    def registries(self, value):
        """Load and check each registry named by a raw or normalized v2 descriptor."""
        found = {}
        families = value.get("families", []) if isinstance(value, dict) else []
        for family in families if isinstance(families, list) else []:
            name = family.get("registry") if isinstance(family, dict) else None
            if isinstance(name, str) and (self.path / "registries" / name).is_file():
                data = (self.path / "registries" / identifier(name)).read_bytes()
                if digest(data) != name:
                    raise ValueError(f"corrupt registries: {name}")
                found[name] = data
        return found

    def plan(self, spec):
        """Generate and partition every key without source access; report planning costs."""
        started = time.monotonic()
        loaded = self.registries(spec)
        spec = descriptor(spec, loaded)
        validated = time.monotonic()
        count = partition_count(spec)
        counts = [0] * count
        key_bytes = [0] * count
        plan_bytes = 0
        for key in keys(spec, loaded):
            partition = partition_of(key, count)
            line = len(canonical(key)) + 1
            plan_bytes += line
            counts[partition] += 1
            key_bytes[partition] += line - 1
        planned = time.monotonic()
        # A record ["key",length,"digest"] adds brackets, commas, 6 assumed length digits,
        # and a quoted 64-character digest to the quoted key: 74 bytes, plus list commas.
        manifest_estimate = sum(k + 74 * n + 2 + max(n - 1, 0) for k, n in zip(key_bytes, counts) if n)
        root = {"format": FORMAT, "descriptor": spec,
                "partitions": [[i, "0" * 64, n, 10 ** 11] for i, n in enumerate(counts) if n]}
        return {"keys": sum(counts), "partitions": count, "active_partitions": sum(1 for n in counts if n),
                "max_partition_keys": max(counts), "min_partition_keys": min(counts),
                "plan_bytes": plan_bytes, "descriptor_bytes": len(canonical(spec)),
                "registry_bytes": sum(len(v) for v in loaded.values()),
                "root_bytes_estimate": len(canonical(root)),
                "manifest_bytes_estimate": manifest_estimate,
                "exceeds_partition_limit": max(counts) > MAX_PARTITION_FILES,
                "validate_s": validated - started, "generate_s": planned - validated,
                "wall_s": time.monotonic() - started}

    def _retain(self, source, key):
        hasher = blake3()
        size = 0
        fd, temporary = tempfile.mkstemp(dir=self._temporary())
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
                progress=None, delta_plan="parent"):
        """Publish from a caller-guaranteed immutable source; reuse completed partitions.

        source_id must name that fixed source version, not a mutable dataset name. The
        local POC cannot enforce this contract. progress is called after durable checkpoints.
        A delta with delta_plan="parent" takes each changed partition's membership from the
        parent manifest and does not generate keys; "full" generates all keys as before.
        """
        if not assume_immutable or not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("publication requires an immutable source assertion and source_id")
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be positive")
        if delta_plan not in ("parent", "full"):
            raise ValueError("delta_plan must be parent or full")
        loaded = self.registries(spec)
        spec = descriptor(spec, loaded)
        count = partition_count(spec)
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
        from_parent = previous is not None and delta_plan == "parent"
        source = Path(source).resolve(strict=True)
        if not source.is_dir():
            raise ValueError("source must be a directory")
        if self.path == source or self.path.is_relative_to(source) or source.is_relative_to(self.path):
            raise ValueError("source and store must be separate trees")
        request = {"format": FORMAT, "descriptor": spec, "source": str(source),
                   "source_id": source_id, "parent": parent, "changed": sorted(changed_keys)}
        if from_parent:
            request["delta_plan"] = "parent"
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
                previous_parts = {p: (value, n, size) for p, value, n, size in
                                  previous["partitions"]} if previous else {}
                changed_parts = {partition_of(key, count) for key in changed_keys}
                plan_paths = [Path(plans) / str(i) for i in range(count)]
                counts = [0] * count
                planning_started = time.monotonic()
                if from_parent:
                    for partition in changed_parts:
                        mine = [k for k in changed_keys if partition_of(k, count) == partition]
                        if partition not in previous_parts:
                            raise ValueError(f"changed keys are outside the descriptor: {sorted(mine)}")
                        parent_keys = {r[0] for r in self._load("manifests", previous_parts[partition][0])}
                        outside = [k for k in mine if k not in parent_keys]
                        if outside:
                            raise ValueError(f"changed keys are outside the descriptor: {sorted(outside)}")
                    for partition, (_value, n, _size) in previous_parts.items():
                        counts[partition] = n
                else:
                    handles = [open(p, "wb") for p in plan_paths]
                    unmatched = set(changed_keys)
                    try:
                        for key in keys(spec, loaded):
                            partition = partition_of(key, count)
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
                planning_s = time.monotonic() - planning_started

                def expected_keys(partition):
                    if from_parent:
                        return [r[0] for r in self._load("manifests", previous_parts[partition][0])]
                    with open(plan_paths[partition], "rb") as plan:
                        return [json.loads(line) for line in plan]

                def process(partition):
                    prior = state["parts"].get(str(partition))
                    if prior:
                        records = self._load("manifests", prior)
                        sentinel = object()
                        for record, key in itertools.zip_longest(records, expected_keys(partition),
                                                                 fillvalue=sentinel):
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
                    touched = set()
                    for key in expected_keys(partition):
                        if previous and key not in changed_keys:
                            record = old_records[key]
                        else:
                            record = self._retain(source, key)
                            source_count += 1
                            source_bytes += record[1]
                            touched.add(self.object_path(record[2]).parent)
                        records.append(record)
                    if self.fanout:
                        for directory in touched:
                            sync_directory(directory)
                    value = self._put_manifest(canonical(records))
                    return partition, value, len(records), sum(r[1] for r in records), False, source_count, source_bytes

                active = [i for i, n in enumerate(counts) if n]
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
                "wall_s": time.monotonic() - started, "planning_s": planning_s,
                "delta_plan": "parent" if from_parent else "full", "retention": "owned-copy",
                "source_consistency": "caller-asserted-immutable"}

    def _partition_records(self, release_id):
        root = self._load("releases", release_id)
        if root["format"] != FORMAT:
            raise ValueError("unsupported release format")

        def load(entry):
            _partition, value, count, size = entry
            records = self._load("manifests", value)
            if len(records) != count or sum(r[1] for r in records) != size:
                raise ValueError("partition totals mismatch")
            return records
        return [lambda entry=entry: load(entry) for entry in root["partitions"]]

    def records(self, release_id):
        for load in self._partition_records(release_id):
            yield from load()

    @staticmethod
    def _selector(prefixes):
        if prefixes is None:
            return lambda key: True
        wanted = {member(p) for p in prefixes}
        return lambda key: key in wanted or any(a in wanted for a in ancestors(key))

    def _each_partition(self, release_id, work, workers):
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be positive")
        loads = self._partition_records(release_id)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            totals = list(executor.map(lambda load: work(load()), loads))
        return [sum(t[0] for t in totals), sum(t[1] for t in totals)]

    def verify(self, release_id, destination=None, *, workers=1, prefixes=None):
        """Read retained bytes, or read the declared files in a restored directory.

        prefixes limits the check to keys equal to or under the given paths.
        """
        selected = self._selector(prefixes)

        def work(records):
            files = size = 0
            for record in records:
                key, expected_size, value = record
                if not selected(key):
                    continue
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
            return files, size
        files, size = self._each_partition(release_id, work, workers)
        return {"ok": True, "release_id": release_id, "files": files, "bytes": size,
                "scope": "declared-members-only" if prefixes is None else "selected-prefixes",
                "check": "fresh-bytes"}

    def restore(self, release_id, destination, *, workers=1, prefixes=None):
        """Restore to a new directory; check each retained object during the copy."""
        destination = Path(destination).absolute()
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(destination)
        if destination == self.path or destination.is_relative_to(self.path):
            raise ValueError("restore destination must be outside the store")
        selected = self._selector(prefixes)
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(dir=destination.parent, prefix=".restore-"))

        def work(records):
            files = size = 0
            for record in records:
                if not selected(record[0]):
                    continue
                output = staging / member(record[0])
                output.parent.mkdir(parents=True, exist_ok=True)
                with open(output, "xb") as stream:
                    size += self._check_record(record, stream)
                files += 1
            return files, size
        try:
            files, size = self._each_partition(release_id, work, workers)
            if destination.exists() or destination.is_symlink():
                raise FileExistsError(destination)
            os.rename(staging, destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
        return {"release_id": release_id, "destination": str(destination), "ok": True,
                "files": files, "bytes": size}


def main(argv=None):
    import sys
    supplied = sys.argv[1:] if argv is None else argv
    if supplied and supplied[0] in {"remote", "push", "fetch", "checkout", "pull"}:
        from .remote_cli import main as remote_main
        return remote_main(supplied)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("remote", "push", "fetch", "checkout", "pull"):
        commands.add_parser(name, help="experimental verified remote workflow")
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
    publish.add_argument("--delta-plan", choices=["parent", "full"], default="parent",
                         help="take delta membership from parent manifests, or generate all keys")
    plan = commands.add_parser("plan", help="generate and partition keys without source access")
    plan.add_argument("store")
    plan.add_argument("descriptor")
    registry = commands.add_parser("registry", help="retain a sorted id registry; print its digest")
    registry.add_argument("store")
    registry.add_argument("path")
    verify = commands.add_parser("verify")
    verify.add_argument("store")
    verify.add_argument("release_id")
    verify.add_argument("--destination")
    verify.add_argument("--workers", type=int, default=1)
    verify.add_argument("--prefix", action="append", help="check only keys at or under this path")
    restore = commands.add_parser("restore")
    restore.add_argument("store")
    restore.add_argument("release_id")
    restore.add_argument("destination")
    restore.add_argument("--workers", type=int, default=1)
    restore.add_argument("--prefix", action="append", help="restore only keys at or under this path")
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
                                   assume_complete_changes=args.assume_complete_changes,
                                   delta_plan=args.delta_plan)
        elif args.command == "plan":
            with open(args.descriptor, "rb") as stream:
                result = store.plan(json.load(stream))
        elif args.command == "registry":
            value, count = store.put_registry(Path(args.path).read_bytes())
            result = {"registry": value, "count": count}
        elif args.command == "verify":
            result = store.verify(args.release_id, args.destination, workers=args.workers,
                                  prefixes=args.prefix)
        else:
            result = store.restore(args.release_id, args.destination, workers=args.workers,
                                   prefixes=args.prefix)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(2, f"computed-release: {error}\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
