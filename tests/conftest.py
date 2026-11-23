"""Shared generic event fixture and explicitly opted-in PostgreSQL test store."""
import os
from uuid import uuid4

import pytest


@pytest.fixture
def db_event():
    return {
        "contract_version": "1", "pipeline_id": "test_" + uuid4().hex, "batch_id": "batch-1",
        "run_id": str(uuid4()), "status": "completed", "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:00:01+00:00", "duration_ms": 1000,
        "reused_from_run_id": None, "error_summary": None,
        "row_counts": {"input_count": 80, "output_count": 56},
        "quality_metrics": {"missing_id": {"count": 11, "denominator": 80, "rate": 0.1375}},
        "properties": {"total_revenue": "7686.31"}, "evidence": {"report": "fixture.json"},
    }


@pytest.fixture
def db_store(db_event):
    from monitoring.store import EventStore, connect, migrate
    dsn = os.environ.get("PIPELINEIQ_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set PIPELINEIQ_TEST_DATABASE_URL to run real PostgreSQL tests")
    from psycopg.conninfo import conninfo_to_dict
    if not conninfo_to_dict(dsn).get("dbname", "").endswith("_test"):
        pytest.fail("Test database name must end with _test")
    migrate(dsn)
    yield EventStore(dsn)
    # Remove only the random pipeline namespace created by this test.
    with connect(dsn) as conn:
        conn.execute("DELETE FROM executions WHERE pipeline_id=%s", (db_event["pipeline_id"],))
