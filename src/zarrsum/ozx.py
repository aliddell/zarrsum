"""Write a pinned remote release as a stored OZX archive."""

import os
from concurrent.futures import ThreadPoolExecutor
from itertools import groupby, islice
from pathlib import Path
from platform import python_version
from tempfile import TemporaryDirectory
from zipfile import ZIP_STORED, ZipFile, ZipInfo

from blake3 import blake3

from .graph_index import Workspace
from .releases import BLOCK, sync_directory
from .remotes import graph, validate


def write_ozx(release_id, get, destination, *, workers=4):
    """Validate remote members and publish a complete archive without a local checkout."""
    if type(workers) is not int or not 1 <= workers <= 8:
        raise ValueError("OZX workers must be between 1 and 8")
    destination = Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    workspace = Workspace()
    try:
        _, content, records = graph(release_id, get, workspace=workspace)
        members = workspace.table()
        for name, _, digest in records.values():
            members[f"{digest}/{name}"] = name
        groups = groupby(members.items(), key=lambda item: item[0].split("/", 1)[0])
        objects = (
            (key, info) for key, info in content.items() if key.startswith("objects/")
        )

        def download(item):
            key, info = item
            data = get(key)
            validate(data, info)
            return info[1], data

        payload_bytes = 0
        object_count = 0
        with TemporaryDirectory(prefix=".ozx-", dir=destination.parent) as directory:
            staging = Path(directory) / "archive.ozx"
            with (
                ZipFile(
                    staging, "x", compression=ZIP_STORED, allowZip64=True
                ) as archive,
                ThreadPoolExecutor(max_workers=workers) as executor,
            ):
                while batch := list(islice(objects, workers)):
                    for digest, data in executor.map(download, batch):
                        group_digest, entries = next(groups)
                        if group_digest != digest:
                            raise ValueError("OZX object and member indexes disagree")
                        for _, name in entries:
                            # A fixed timestamp makes rebuilds independent of the clock.
                            archive.writestr(ZipInfo(name), data)
                        payload_bytes += len(data)
                        object_count += 1
            if next(groups, None) is not None:
                raise ValueError("OZX is missing an object group")
            checksum = blake3()
            with staging.open("rb") as stream:
                for block in iter(lambda: stream.read(BLOCK), b""):
                    checksum.update(block)
                archive_bytes = stream.tell()
                os.fchmod(stream.fileno(), 0o400)
                os.fsync(stream.fileno())
            # A hard link publishes atomically and refuses to replace any destination.
            os.link(staging, destination)
            sync_directory(destination.parent)

        return {
            "release_id": release_id,
            "result": "verified",
            "coverage": "full",
            "destination": str(destination),
            "files": len(records),
            "downloaded_objects": object_count,
            "downloaded_bytes": payload_bytes,
            "archive_bytes": archive_bytes,
            "archive_blake3": checksum.hexdigest(),
            "packaging": {
                "format": "ozx",
                "method": "ZIP_STORED",
                "allow_zip64": True,
                "order": "digest-then-path",
                "writer": f"python {python_version()} zipfile",
            },
        }
    finally:
        workspace.close()
