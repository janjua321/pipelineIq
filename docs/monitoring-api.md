# Monitoring API (B3)

The API accepts the same contract-1 event that B1 exports and passes it to B2's
transactional store. It does not run Spark, read evidence paths, modify Delta
outputs or trigger a pipeline. PostgreSQL remains the source for queries.

## Start and use

Install `.[dev,monitoring,api]` using the README's pip or uv environment setup.
Verified pins: FastAPI 0.165.0, Pydantic 2.14.4, Uvicorn 0.54.0 and Psycopg 3.3.9.
HTTP tests use HTTPX 0.28.1. Pydantic models describe the request/response fields;
semantic validation delegates to B1's `validate_event` rather than copying retail
rules into the API. See FastAPI's official [body model documentation](https://fastapi.tiangolo.com/tutorial/body/).

```bash
bash scripts/postgres.sh start
export PIPELINEIQ_DATABASE_URL="host=$PWD/.cache/postgres-socket port=55432 dbname=pipelineiq"
.venv/bin/python -m monitoring.cli migrate
bash scripts/api.sh
```

Leave that terminal running. The script starts
`python -m uvicorn api.app:create_app --factory --host 127.0.0.1 --port 8000`.
A factory constructs an application with the configured database connection string.
No connection string is hardcoded or logged. Missing configuration fails startup
with a clear message; an unavailable database produces 503 when accessed. Migrations
are explicit and are never run automatically during HTTP startup or submission.

From a second terminal in the repository root:

```bash
curl --fail-with-body http://127.0.0.1:8000/health/ready
for event in data/monitoring/events/retail_transactions/*.json; do
  curl --fail-with-body -H 'Content-Type: application/json' \
    --data-binary "@$event" http://127.0.0.1:8000/v1/runs
done
curl --fail-with-body 'http://127.0.0.1:8000/v1/runs?pipeline_id=retail_transactions&limit=20'
curl --fail-with-body 'http://127.0.0.1:8000/v1/runs?status=reused'
```

For exact retrieval, use `/v1/runs/<pipeline-id>/<run-id>` with an ID from the list
response. Interactive API documentation is at `http://127.0.0.1:8000/docs`, and
`/openapi.json` describes the machine-readable contract. Ctrl-C stops the API;
`bash scripts/postgres.sh stop` separately stops the project database without
removing records. A new terminal needs its own environment-variable export.

## Routes and outcomes

| Route | Behavior |
| --- | --- |
| `POST /v1/runs` | Accept one JSON execution snapshot; 201 created, 200 updated/unchanged |
| `GET /v1/runs/{pipeline_id}/{run_id}` | Return the stored event; 404 if absent |
| `GET /v1/runs` | Return `{items, limit, offset}` with optional pipeline/status filters |
| `GET /health/ready` | Check database connectivity and required tables; 200 ready or 503 |

The submission receipt contains `outcome`, `pipeline_id` and `run_id`. Sending an
already stored demo event normally returns 200 unchanged, because the B2 CLI and
B3 API share the same database. On an empty database its first submission is 201.
The same running-to-terminal rules apply through either interface. See the
[storage guide](monitoring-storage.md) for concurrency, conflicts and retention.

List bounds are limit 1–100 and offset 0–100000. Optional status is one of running,
completed, completed_with_warnings, failed or reused. Results sort by start time
descending, then pipeline/run identity. Pagination is a convenience for inspection,
not B5's history comparison or an aggregate revenue report. Concurrent insertions
can shift offset pages.

| Status | Meaning and investigation |
| --- | --- |
| 400 | Malformed JSON, duplicate keys, non-finite constants or invalid Unicode; fix the serialized document |
| 413 | Body exceeds 1 MiB; reduce the event, keeping large evidence in referenced files |
| 415 | Use `Content-Type: application/json` |
| 422 | Invalid event/query/path values, or text PostgreSQL cannot represent; inspect the field error |
| 409 | Changed running snapshot, terminal overwrite or changed immutable metadata; inspect the existing execution |
| 404 | No execution exists at the supplied pipeline/run identity |
| 503 | Database unreachable or schema unavailable; check cluster status, DSN and explicit migrations |

Input is streamed with a size cap before parsing, including when Content-Length
is absent. Strict JSON parsing rejects duplicate keys before Pydantic can discard
them. Errors omit raw request values and database connection details. Event counts
remain strict integers: `true` and `"80"` are not accepted as counts. Out-of-range
UTC conversion is a validation error, not an unhandled server exception.

## Code path and boundaries

```text
scripts/api.sh → Uvicorn → api.app.create_app()
POST /v1/runs
  → parse_event: media type, body size, strict JSON
  → ExecutionEvent: Pydantic shape + B1 semantic validation
  → EventStore.submit: validate, lock identity, transaction, persist
  → created / updated / unchanged receipt
GET /v1/runs...
  → parameter validation → EventStore query → response model
```

Synchronous database handlers run in FastAPI's worker thread pool. Each store
operation opens and closes one connection, with connection/statement/lock timeouts.
There is no connection pool or retry queue yet. A caller may retry an uncertain
submission safely with the identical event. Evidence paths are opaque references;
the server never executes them.

The supplied server binds to loopback. Authentication, authorization, TLS, quotas,
shared-user isolation and public deployment are not implemented. This is a trusted
local development service: do not expose it publicly as configured. It exposes no
delete or pipeline-execution endpoint. No API server is left running by the demo
verification; start it with the command above when you want to use it.

## Verify and learn

```bash
export PIPELINEIQ_TEST_DATABASE_URL="host=$PWD/.cache/postgres-socket port=55432 dbname=pipelineiq_test"
.venv/bin/python -m pytest -q tests/integration/test_api.py tests/integration/test_store.py
bash scripts/test.sh
```

Tests use an HTTP test client backed by real PostgreSQL, covering status codes,
invalid requests without writes, lifecycle conflicts, pagination bounds, readiness,
body limits and database-unavailable errors. A separate live Uvicorn demonstration
submits/gets the three saved measured events and checks filtering and OpenAPI.
Actual counts/timings are recorded in [PROGRESS.md](PROGRESS.md).

Exercise: POST an exported event twice and explain its receipt. Compare the
response to posting a changed terminal event with the same identity (409). Then
query `status=reused` and explain why its revenue is not new processing. Preserve
original exported files; use a separate copy for intentional invalid examples.

B4 [Airflow orchestration](orchestration.md) and B5 [history reporting](history.md)
are now implemented as separate entry points. The DAG publishes directly through
the same store; it does not require the HTTP service. B5 is a CLI report, not a
new HTTP endpoint. Next is C1 after Milestone B review.
