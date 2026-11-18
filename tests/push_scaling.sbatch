#!/bin/bash
#SBATCH --partition=cpu
#SBATCH --qos=low
#SBATCH --exclusive
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=00:45:00
#SBATCH --no-requeue
#SBATCH --job-name="dv-push-scale - Katamari"
set -euo pipefail
umask 077
TASK_ROOT=$1
CONTROL="$TASK_ROOT/run-$SLURM_JOB_ID/control"
mkdir -p "$CONTROL"
trap 'rm -f "$CONTROL/session.json"' EXIT
export PYTHONDONTWRITEBYTECODE=1
unset PYTHONPATH
srun --kill-on-bad-exit=0 --nodes=2 --ntasks=2 --ntasks-per-node=1 "$TASK_ROOT/test-venv/bin/python" "$TASK_ROOT/zarrsum/tests/push_scaling.py" "$TASK_ROOT" "$CONTROL"
