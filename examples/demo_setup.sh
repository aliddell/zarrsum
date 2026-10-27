#!/usr/bin/env bash
# Set up a venv, build a tiny Zarr v2 fixture, and run the end-to-end dvc_demo.sh.
#
#   demo_setup.sh [workdir]        # default workdir: ./zarrsum-demo
#
# Needs `uv` and `git`. Installs zarrsum (editable, from this checkout), blake3, and dvc
# into a fresh venv under the workdir, so the demo is self-contained.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
WORK="${1:-$PWD/zarrsum-demo}"

rm -rf "$WORK"; mkdir -p "$WORK"; cd "$WORK"
uv venv --python 3.12 .venv
. .venv/bin/activate
uv pip install -e "$REPO" blake3 dvc

# a tiny Zarr v2 store: one group, one array of four chunks
python -I - "$WORK/fixture/data" <<'PY'
import json, os, sys
r = sys.argv[1]
os.makedirs(os.path.join(r, "arr"), exist_ok=True)
json.dump({"zarr_format": 2}, open(os.path.join(r, ".zgroup"), "w"))
json.dump({"zarr_format": 2, "shape": [4, 4], "chunks": [2, 2], "dtype": "<i4",
           "fill_value": 0, "order": "C", "filters": None, "compressor": None,
           "dimension_separator": "."}, open(os.path.join(r, "arr", ".zarray"), "w"))
for i in range(2):
    for j in range(2):
        open(os.path.join(r, "arr", f"{i}.{j}"), "wb").write(bytes([i * 2 + j]) * 16)
PY

bash "$REPO/examples/dvc_demo.sh" "$WORK/fixture/data" "$WORK/run"
