"""Measure only missing-content bookkeeping, not transfer or full preparation."""

import json
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import time
import tracemalloc

from zarrsum.graph_index import Workspace
from zarrsum.locations import _missing


def child(count):
    class Content:
        def items(self):
            for number in range(count):
                value = f"{number:064x}"
                yield f"objects/{value[:2]}/{value[2:4]}/{value}", [1, value]

    with tempfile.TemporaryDirectory(prefix="prep-memory-cache-") as directory:
        workspace = Workspace()
        scratch = Path(workspace.directory.name)
        baseline = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        started = time.monotonic()
        try:
            tracemalloc.start()
            missing = _missing(Content(), Path(directory), workspace)
            _, python_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            assert len(missing) == count
            assert python_peak < 4 << 20
            result = {
                "entries": count,
                "python_peak_bytes": python_peak,
                "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "baseline_peak_rss_kib": baseline,
                "database_bytes": (scratch / "graph.sqlite").stat().st_size,
                "wall_s": time.monotonic() - started,
            }
        finally:
            workspace.close()
        result["scratch_removed"] = not scratch.exists()
    print(json.dumps(result))


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--child":
        child(int(sys.argv[2]))
        return
    rows = [
        json.loads(
            subprocess.check_output(
                [sys.executable, __file__, "--child", str(count)], text=True
            )
        )
        for count in (1000, 100000)
    ]
    assert all(row["scratch_removed"] for row in rows)
    assert rows[1]["peak_rss_kib"] - rows[0]["peak_rss_kib"] < 24 << 10
    Path(sys.argv[1]).write_text(
        json.dumps({"pass": True, "scope": "bookkeeping-only", "rows": rows}, indent=2)
    )


if __name__ == "__main__":
    main()
