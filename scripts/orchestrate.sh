#!/usr/bin/env bash
set -euo pipefail
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
if [[ -z "${JAVA_HOME:-}" && -x .tools/jdk-21/bin/java ]]; then
    export JAVA_HOME="$project_dir/.tools/jdk-21"
fi
if [[ -n "${JAVA_HOME:-}" ]]; then export PATH="$JAVA_HOME/bin:$PATH"; fi
exec .venv/bin/python -m orchestration.worker "$@"
