#!/usr/bin/env bash
# Run the DVC + zarrsum demo against a real Zarr store, timed, and report throughput.
#
#   demo_real_store.sh <store_path> <scratch_workdir>
#
# The source store is never written: it is copied into the scratch workdir first. Each step
# is timed; the script then computes size/time and files/time for the ingest (`dvc add`) and
# the consumer pull, and writes result.json. Needs `dvc`, `git`, and zarrsum importable
# (`python -m zarrsum.manifest`); prepare that first (the sbatch wrapper or demo_setup.sh does).
# On Bruno, run this under an exclusive SLURM node, not the shared login node, so the timings
# are not contended.
set -euo pipefail
STORE="${1:?usage: demo_real_store.sh <store_path> <scratch_workdir>}"
WORK="${2:?usage: demo_real_store.sh <store_path> <scratch_workdir>}"
export DVC_NO_ANALYTICS=1 GIT_TERMINAL_PROMPT=0

store_bytes=$(du -sb "$STORE" | cut -f1)
file_count=$(find "$STORE" -type f | wc -l)
rm -rf "$WORK"; mkdir -p "$WORK/repo" "$WORK/remote"
: > "$WORK/timings.txt"

t() {  # t <label> <cmd...>: run the command and record its wall seconds
  local label="$1"; shift
  local t0 t1; t0=$(date +%s.%N); "$@"; t1=$(date +%s.%N)
  awk "BEGIN{printf \"%s %.3f\n\", \"$label\", $t1-$t0}" >> "$WORK/timings.txt"
}

cd "$WORK/repo"
t cp cp -a "$STORE" data
git init -q -b main; git config user.email demo@local; git config user.name demo
dvc init -q; dvc config core.analytics false
t dvc_add dvc add data
python -m zarrsum.manifest add manifest.json training_data data --policy bytes
dvc remote add -d store "$WORK/remote"
git add -A; git commit -q -m "track data; pin zarrsum id"
t dvc_push dvc push -q
cd "$WORK"; t clone git clone -q repo clone; cd clone
t dvc_pull dvc pull -q
t verify python -m zarrsum.manifest verify manifest.json training_data data

python -I - "$store_bytes" "$file_count" "$WORK/timings.txt" "$WORK/result.json" <<'PY'
import json, sys
store_bytes, file_count = int(sys.argv[1]), int(sys.argv[2])
times = {}
for line in open(sys.argv[3]):
    label, secs = line.split()
    times[label] = float(secs)
gib = store_bytes / 2**30
def rate(sec):
    return {"gib_per_s": round(gib / sec, 3), "files_per_s": round(file_count / sec, 1)} if sec else None
total = round(sum(times.values()), 3)
out = {"store_bytes": store_bytes, "store_gib": round(gib, 2), "file_count": file_count,
       "seconds": {**times, "total": total},
       "throughput": {"dvc_add": rate(times.get("dvc_add")),
                      "dvc_pull": rate(times.get("dvc_pull")),
                      "total": rate(total)}}
json.dump(out, open(sys.argv[4], "w"), indent=2)
print(json.dumps(out, indent=2))
PY
echo "RESULT written to $WORK/result.json"
