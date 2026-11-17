#!/bin/bash
#SBATCH --partition=cpu
#SBATCH --qos=low
#SBATCH --exclusive
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=00:20:00
#SBATCH --no-requeue
#SBATCH --job-name="dv-two-host-s3 - Katamari"
set -euo pipefail
umask 077
TASK_ROOT=$1
CONTROL="$TASK_ROOT/run-$SLURM_JOB_ID/control"
mkdir -p "$CONTROL"
trap 'rm -f "$CONTROL/session.json"' EXIT
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$TASK_ROOT/zarrsum/src:$TASK_ROOT/hermes/packages/core/src"
srun --kill-on-bad-exit=0 --nodes=2 --ntasks=2 --ntasks-per-node=1 "$TASK_ROOT/venv/bin/python" "$TASK_ROOT/zarrsum/tests/two_host_s3.py" "$TASK_ROOT" "$CONTROL"
