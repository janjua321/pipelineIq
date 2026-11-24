#!/usr/bin/env bash
set -euo pipefail
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
export PIPELINEIQ_PROJECT="$project_dir"
export AIRFLOW_HOME="$project_dir/data/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$project_dir/orchestration/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
if [[ "${1:-}" == python ]]; then
  shift
  exec .tools/airflow-venv/bin/python "$@"
fi
exec .tools/airflow-venv/bin/airflow "$@"
