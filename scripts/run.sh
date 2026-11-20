#!/usr/bin/env bash
# Select the local runtime, then run the full pipeline CLI.
set -euo pipefail
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
if [[ ! -x .venv/bin/python ]]; then
    printf '%s\n' 'Missing .venv/bin/python. Follow the setup steps in README.md.' >&2
    exit 1
fi
if [[ -z "${JAVA_HOME:-}" && -x .tools/jdk-21/bin/java ]]; then
    export JAVA_HOME="$project_dir/.tools/jdk-21"
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="$JAVA_HOME/bin:$PATH"
fi
exec .venv/bin/python -m pipelines.run "$@"
