# PostgreSQL monitoring storage (B2)

B2 stores validated contract-1 events without importing Spark or retail logic.
`monitoring/cli.py` is the command-line entry point; `monitoring/store.py` owns
parameterized SQL, transactions, lifecycle rules and queries. The SQL migration
is packaged with the application so installed commands work outside the checkout.

## Setup and commands

The verified environment uses PostgreSQL 18.6 and Python 3.12 with Psycopg 3.3.9.
Install `.[dev,monitoring]` using the README's pip or uv setup. Psycopg's binary
extra supplies its client library; it does not install a PostgreSQL server.
Compatibility and transaction behavior were checked against the official
[Psycopg installation](https://www.psycopg.org/psycopg3/docs/basic/install.html) and
[transaction documentation](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).

```bash
bash scripts/postgres.sh start
export PIPELINEIQ_DATABASE_URL="host=$PWD/.cache/postgres-socket port=55432 dbname=pipelineiq"
.venv/bin/python -m monitoring.cli migrate
for event in data/monitoring/events/retail_transactions/*.json; do
  .venv/bin/python -m monitoring.cli submit --event "$event"
done
.venv/bin/python -m monitoring.cli list --pipeline-id retail_transactions --limit 20
.venv/bin/python -m monitoring.cli list --status reused
bash scripts/postgres.sh status
# When finished using the database:
bash scripts/postgres.sh stop
```

`get --pipeline-id <id> --run-id <uuid>` retrieves an exact event; missing executions
exit 1. Invalid input/database errors exit 2. Database connection details are not
printed in errors. Commands require `PIPELINEIQ_DATABASE_URL`; they do not load
`.env` automatically. `.env.example` documents the configuration.

The script creates `data/postgres` and `.cache/postgres-socket`, plus separate
`pipelineiq` and `pipelineiq_test` databases. It never deletes a cluster. The socket
directory is accessible only to its owner; local connections trust that OS user.
TCP is disabled. Stop affects only this project's cluster and retains its data.
This setup targets Linux/Unix development; a remote/shared database requires its
own authentication, roles and backup configuration. Do not reuse these local trust
settings for a network listener. The script expects server tools on PATH.

## Tables and transactions

| Table | Responsibility |
| --- | --- |
| `schema_migrations` | Applied SQL filenames, SHA-256 checksums and application times |
| `executions` | Composite pipeline/run identity, searchable status/times/batch, full JSONB event and receipt/update times |
| `row_counts` | Named counts, retaining unknown SQL NULL values |
| `quality_metrics` | Named counts, denominators and rates; null rates for unknown/empty populations |

The execution key is `(pipeline_id, run_id)`. Child metric rows use that key plus
the metric name, with foreign keys and cascading deletion. A single transaction
writes both the event and its measurements: a metric failure rolls back even an
already-written execution update. JSONB preserves the event structure and decimal
revenue strings; the relational tables support future SQL analysis. Application
writes go through `EventStore`; direct manual SQL can bypass event-level checks.

`INSERT ... ON CONFLICT DO NOTHING` establishes one execution identity under
concurrency. A subsequent `SELECT ... FOR UPDATE` serializes updates to an existing
execution. See PostgreSQL's [INSERT behavior](https://www.postgresql.org/docs/18/sql-insert.html).
These are database locks, distinct from A6's filesystem batch locks.

## Delivery and lifecycle rules

| Existing snapshot | Incoming snapshot | Outcome |
| --- | --- | --- |
| Absent | Valid event, including terminal | `created` |
| Any | Canonically identical event | `unchanged`; timestamps/metrics are not rewritten |
| Running | Terminal, same contract/batch/start time | `updated` atomically |
| Running | Different running snapshot | Conflict: contract 1 has no ordering revision |
| Terminal | Any changed snapshot, including stale running | Conflict |

Identity, contract version, batch label and start timestamp cannot change during
an update. Terminal reports may arrive without a prior running report. Conflict
leaves original evidence untouched; a correction needs an explicit future policy,
not a silent overwrite. An original event may arrive after its reused event, so
`reused_from_run_id` is retained in JSON but is not a database foreign key.

Queries support optional pipeline/status filters and `limit` 1–100 (default 20),
`offset` 0–100000. Ordering is start time descending, then pipeline and run ID.
Offset pages are deterministic on a fixed dataset, but concurrent insertions can
shift page boundaries. No cross-request snapshot or cursor guarantee is made.
No cross-run totals are computed here; a later history report must exclude reused
executions from newly processed totals and processing-time baselines.

## Migration and retention policy

Run migrations explicitly before using storage. An advisory transaction lock
serializes migrators. Applied files are never edited: checksums detect changed
DDL, and unknown applied versions are rejected. Add a numbered SQL file for a new
schema change. Migration DDL and its ledger entry commit together. This first
migration is additive; there is no automatic downgrade/reset command.

Retention is indefinite for this local learning dataset. There is no automatic
purge, scheduling or deletion API. Before adding retention, choose an archive/backup
policy and a cutoff based on finished/received time; account for reused references
whose original event might be removed. Future explicitly authorized deletions of
execution rows will cascade to named metric rows. This store keeps current execution
snapshots, not every running-state revision. It does not delete source files or
Delta outputs. Backups and restore automation are not implemented.

## Verification

```bash
export PIPELINEIQ_TEST_DATABASE_URL="host=$PWD/.cache/postgres-socket port=55432 dbname=pipelineiq_test"
.venv/bin/python -m pytest -q tests/integration/test_store.py
bash scripts/test.sh
```

Database tests require a DSN whose explicit database name ends with `_test`. They
create random pipeline IDs and delete only those test rows afterward. Without the
DSN the database tests skip; skipping is not proof of database compatibility. With
no monitoring extra installed the storage test module also skips. The executed B2
checkpoint passed nine tests against PostgreSQL 18.6. Tests cover migration replay
and checksum protection, exact retrieval, null/empty measurements, concurrent
inserts/updates, rollback and filtering. The three measured demo events were stored,
resubmitted unchanged, and retrieved with their original contents.
