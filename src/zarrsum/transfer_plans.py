"""Pin bounded transfer plans without increasing the release graph limit.

An index covers the complete nonroot graph. Child paths derive only from digests.
The caller checks remote bytes and publishes the release root after all transfers.
"""

from pathlib import Path
import json

from .releases import atomic_file, canonical, digest, identifier

MAX_ENTRIES = 10_000
MAX_PLANS = 64
INDEX_BYTES = 64 << 10
PLAN_BYTES = 32 << 20


def pinned_bytes(path, pinned, limit):
    identifier(pinned)
    with Path(path).open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit or digest(raw) != pinned:
        raise ValueError("transfer plan changed or exceeds byte limit")
    return raw


def pinned_document(path, pinned, limit):
    value = json.loads(pinned_bytes(path, pinned, limit))
    if not isinstance(value, dict):
        raise ValueError("invalid transfer plan document")
    return value


def create_index(path, release_id, source, destination, repository, expected):
    if not expected or len(expected) > MAX_ENTRIES * MAX_PLANS:
        raise ValueError("transfer plan index count exceeds limit")
    entries = [[key, *info] for key, info in sorted(expected.items())]
    plans = []
    for offset in range(0, len(entries), MAX_ENTRIES):
        part = entries[offset : offset + MAX_ENTRIES]
        raw = canonical(
            {
                "version": 1,
                "release_id": release_id,
                "source": source,
                "destination": destination,
                "entries": part,
            }
        )
        if len(raw) > PLAN_BYTES:
            raise ValueError("transfer child plan exceeds byte limit")
        pinned = digest(raw)
        child = Path(path).parent / "plans" / (pinned + ".json")
        child.parent.mkdir(parents=True, exist_ok=True)
        if child.exists():
            pinned_document(child, pinned, PLAN_BYTES)
        else:
            atomic_file(child, raw)
        plans.append([pinned, len(part)])
    raw = canonical(
        {
            "version": 2,
            "release_id": release_id,
            "source": source,
            "destination": destination,
            "repository": repository,
            "entries_count": len(entries),
            "plans": plans,
        }
    )
    if len(raw) > INDEX_BYTES:
        raise ValueError("transfer plan index exceeds byte limit")
    atomic_file(path, raw)
    return digest(raw)


def validate_index(path, pinned, release_id, source, destination, repository, expected):
    document = pinned_document(path, pinned, INDEX_BYTES)
    if (
        set(document)
        != {
            "version",
            "release_id",
            "source",
            "destination",
            "repository",
            "entries_count",
            "plans",
        }
        or type(document["version"]) is not int
        or document["version"] != 2
        or document["release_id"] != release_id
        or document["source"] != source
        or not Path(source).is_absolute()
        or document["destination"] != destination
        or document["repository"] != repository
        or type(document["entries_count"]) is not int
        or document["entries_count"] != len(expected)
        or not isinstance(document["plans"], list)
        or not 1 <= len(document["plans"]) <= MAX_PLANS
    ):
        raise ValueError(
            "transfer plan index does not match the release and repositories"
        )
    seen, children, hashes = set(), [], set()
    previous = None
    for reference in document["plans"]:
        if (
            not isinstance(reference, list)
            or len(reference) != 2
            or not isinstance(reference[0], str)
            or type(reference[1]) is not int
            or not 1 <= reference[1] <= MAX_ENTRIES
        ):
            raise ValueError("invalid child plan reference")
        child_pin, count = reference
        identifier(child_pin)
        if child_pin in hashes:
            raise ValueError("duplicate child plan reference")
        hashes.add(child_pin)
        child = Path(path).parent / "plans" / (child_pin + ".json")
        plan = pinned_document(child, child_pin, PLAN_BYTES)
        if (
            set(plan) != {"version", "release_id", "source", "destination", "entries"}
            or type(plan["version"]) is not int
            or plan["version"] != 1
            or plan["release_id"] != release_id
            or plan["source"] != source
            or plan["destination"] != destination
            or not isinstance(plan["entries"], list)
            or len(plan["entries"]) != count
        ):
            raise ValueError("child plan does not match index")
        for entry in plan["entries"]:
            if (
                not isinstance(entry, list)
                or len(entry) != 3
                or not isinstance(entry[0], str)
                or entry[0] not in expected
                or type(entry[1]) is not int
                or entry[1] < 0
                or not isinstance(entry[2], str)
                or entry[1:] != list(expected[entry[0]])
                or entry[0] in seen
                or (previous is not None and entry[0] <= previous)
            ):
                raise ValueError("child plans do not cover the exact nonroot graph")
            seen.add(entry[0])
            previous = entry[0]
        children.append((child, child_pin))
    if seen != set(expected):
        raise ValueError("child plans omit required graph objects")
    return children
