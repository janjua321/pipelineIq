#!/usr/bin/env bash
set -euo pipefail
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
uv="$project_dir/.tools/bootstrap/bin/uv"
export UV_CACHE_DIR="$project_dir/.cache/uv"
if [[ ! -x .tools/airflow-venv/bin/python ]]; then
  "$uv" venv --python .venv/bin/python .tools/airflow-venv
fi
"$uv" pip install --python .tools/airflow-venv/bin/python \
  -r orchestration/requirements-airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt
