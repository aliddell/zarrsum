"""Keep graph-sized membership and content indexes on bounded private scratch.

SQLite uses a fixed page cache and a bounded database. JSON metadata remains bounded
per document. These indexes are disposable working state, never release identity.
"""

from collections.abc import Mapping, MutableMapping, MutableSet
import json
from pathlib import Path
import sqlite3
import tempfile

CACHE_KIB = 8192
SCRATCH_BYTES = 2 << 30


class Workspace:
    def __init__(self):
        self.directory = tempfile.TemporaryDirectory(prefix="zarrsum-graph-")
        self.connection = sqlite3.connect(
            str(Path(self.directory.name) / "graph.sqlite")
        )
        self.connection.execute("PRAGMA page_size=4096")
        self.connection.execute(f"PRAGMA max_page_count={SCRATCH_BYTES // 4096}")
        self.connection.execute(f"PRAGMA cache_size=-{CACHE_KIB}")
        self.connection.execute("PRAGMA mmap_size=0")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.serial = 0

    def table(self, binary=False):
        self.serial += 1
        name = "map_" + str(self.serial)
        self.connection.execute(
            f"CREATE TABLE {name} (k TEXT PRIMARY KEY, v BLOB) WITHOUT ROWID"
        )
        return DiskMap(self, name, binary)

    def set(self):
        return DiskSet(self.table())

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self.directory.cleanup()

    def __del__(self):
        if getattr(self, "connection", None) is not None:
            self.close()


class DiskMap(MutableMapping):
    def __init__(self, workspace, table, binary=False):
        self.workspace, self.table, self.binary = workspace, table, binary
        self.count = 0

    def __getitem__(self, key):
        row = self.workspace.connection.execute(
            f"SELECT v FROM {self.table} WHERE k=?", (key,)
        ).fetchone()
        if row is None:
            raise KeyError(key)
        return row[0] if self.binary else json.loads(row[0])

    def __contains__(self, key):
        return (
            self.workspace.connection.execute(
                f"SELECT 1 FROM {self.table} WHERE k=?", (key,)
            ).fetchone()
            is not None
        )

    def __setitem__(self, key, value):
        if key not in self:
            self.count += 1
        encoded = value if self.binary else json.dumps(value, separators=(",", ":"))
        self.workspace.connection.execute(
            f"INSERT OR REPLACE INTO {self.table} VALUES (?,?)", (key, encoded)
        )

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.workspace.connection.execute(f"DELETE FROM {self.table} WHERE k=?", (key,))
        self.count -= 1

    def __iter__(self):
        for row in self.workspace.connection.execute(
            f"SELECT k FROM {self.table} ORDER BY k"
        ):
            yield row[0]

    def __len__(self):
        return self.count

    def items(self):
        for key, raw in self.workspace.connection.execute(
            f"SELECT k,v FROM {self.table} ORDER BY k"
        ):
            yield key, raw if self.binary else json.loads(raw)

    ordered_items = items

    def values(self):
        for _, value in self.items():
            yield value


class DiskSet(MutableSet):
    def __init__(self, mapping):
        self.mapping = mapping

    def __contains__(self, key):
        return key in self.mapping

    def __len__(self):
        return len(self.mapping)

    def __iter__(self):
        return iter(self.mapping)

    def add(self, key):
        self.mapping[key] = None

    def discard(self, key):
        if key in self:
            del self.mapping[key]

    def update(self, values):
        for value in values:
            self.add(value)


class WithoutRoot(Mapping):
    def __init__(self, mapping, root):
        self.mapping, self.root = mapping, root

    def __getitem__(self, key):
        if key == self.root:
            raise KeyError(key)
        return self.mapping[key]

    def __contains__(self, key):
        return key != self.root and key in self.mapping

    def __iter__(self):
        return (key for key in self.mapping if key != self.root)

    def __len__(self):
        return len(self.mapping) - int(self.root in self.mapping)

    def items(self):
        return ((key, value) for key, value in self.mapping.items() if key != self.root)

    ordered_items = items


class VerifiedKeys:
    def __init__(self, required, missing):
        self.required, self.missing = required, missing

    def __contains__(self, key):
        return key in self.required and key not in self.missing

    def __len__(self):
        return len(self.required) - len(self.missing)


class EvidenceLog(list):
    """Spool full evidence to disk and retain only a bounded receipt preview."""

    def __init__(self, path, preview=256, byte_limit=64 << 20):
        super().__init__()
        self.path, self.preview, self.byte_limit = Path(path), preview, byte_limit
        self.count, self.bytes = 0, 0
        self.path.write_bytes(b"")

    def append(self, value):
        raw = (json.dumps(value, separators=(",", ":")) + "\n").encode()
        if self.bytes + len(raw) > self.byte_limit:
            raise ValueError("evidence log exceeds byte budget")
        with self.path.open("ab") as stream:
            stream.write(raw)
        self.bytes += len(raw)
        self.count += 1
        if len(self) < self.preview:
            super().append(value)
