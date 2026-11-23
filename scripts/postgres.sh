#!/usr/bin/env bash
# Project-owned local PostgreSQL cluster. Unix socket only; no system service changes.
set -euo pipefail
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
cluster="$project_dir/data/postgres"
socket_dir="$project_dir/.cache/postgres-socket"
port=55432
case "${1:-}" in
  start)
    command -v initdb >/dev/null || { echo 'Install PostgreSQL server tools first.' >&2; exit 1; }
    mkdir -p "$socket_dir" data
    chmod 700 "$socket_dir"
    if [[ ! -f "$cluster/PG_VERSION" ]]; then
      initdb -D "$cluster" --auth-local=trust --auth-host=reject --encoding=UTF8 --locale=C
    fi
    if ! pg_ctl -D "$cluster" status >/dev/null 2>&1; then
      pg_ctl -D "$cluster" -l "$project_dir/.cache/postgres.log" \
        -o "-k '$socket_dir' -p $port -c listen_addresses='' -c unix_socket_permissions=0700" -w start
    fi
    for database in pipelineiq pipelineiq_test; do
      if [[ "$(psql -X -h "$socket_dir" -p "$port" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='$database'")" != 1 ]]; then
        createdb -h "$socket_dir" -p "$port" "$database"
      fi
    done
    printf 'Ready. Set PIPELINEIQ_DATABASE_URL="host=%s port=%s dbname=pipelineiq"\n' "$socket_dir" "$port"
    ;;
  stop) pg_ctl -D "$cluster" -m fast -w stop ;;
  status) pg_ctl -D "$cluster" status ;;
  *) echo 'Usage: bash scripts/postgres.sh {start|stop|status}' >&2; exit 2 ;;
esac
