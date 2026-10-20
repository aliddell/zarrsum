#!/usr/bin/env bash
# End-to-end demo: DVC moves the bytes, zarrsum pins and verifies the identity.
#
#   dvc_demo.sh <dataset_path> <workdir>
#
# Needs `dvc`, `git`, and zarrsum importable (pip install zarrsum, or set PYTHONPATH to
# .../zarrsum/src). Uses `python -m zarrsum.manifest` so it works either way.
set -euo pipefail
DATASET="${1:?usage: dvc_demo.sh <dataset_path> <workdir>}"
WORK="${2:?usage: dvc_demo.sh <dataset_path> <workdir>}"
export DVC_NO_ANALYTICS=1 GIT_TERMINAL_PROMPT=0

rm -rf "$WORK"; mkdir -p "$WORK/repo" "$WORK/remote"
cd "$WORK/repo"
cp -a "$DATASET" data
git init -q -b main; git config user.email demo@local; git config user.name demo
dvc init -q; dvc config core.analytics false

echo "== producer: DVC tracks the bytes; the manifest pins the zarrsum id =="
dvc add data                                             # DVC: one .dir pointer + cache
python -m zarrsum.manifest add manifest.json training_data data --policy inventory
dvc remote add -d store "$WORK/remote"
git add -A; git commit -q -m "track data; pin zarrsum id"
dvc push -q                                              # bytes -> remote (stands in for S3/Reef)

echo "== consumer: fresh clone, pull the bytes, verify identity =="
cd "$WORK"; git clone -q repo clone; cd clone
dvc pull -q                                              # bytes <- remote
python -m zarrsum.manifest verify manifest.json training_data data
echo "== tamper check: change one byte, verify must FAIL =="
# (bytes-policy sub-check on a copy, to show verification has teeth)
python - <<'PY'
import json, os, glob, sys
sys.path.insert(0, os.environ.get("ZARRSUM_SRC", ""))
from zarrsum import manifest
# re-pin under the bytes policy, then flip a chunk and confirm verify fails
manifest.add_dataset("manifest_bytes.json", "td", "data", policy="bytes")
chunks = [p for p in glob.glob("data/**", recursive=True)
          if os.path.isfile(p) and os.path.basename(p).replace(".", "").isdigit()]
with open(chunks[0], "r+b") as f:
    b = f.read(1); f.seek(0); f.write(bytes([b[0] ^ 1]))  # flip one bit, same size
r = manifest.verify_dataset("manifest_bytes.json", "td")
print("bytes-policy verify after 1-bit flip:", "FAIL (good)" if not r["ok"] else "OK (unexpected!)")
assert not r["ok"]
PY
echo "DEMO OK"
