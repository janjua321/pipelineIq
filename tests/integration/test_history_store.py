from uuid import uuid4

import pytest

pytest.importorskip("psycopg")
from monitoring.history import history

pytestmark = pytest.mark.postgres


def test_bounded_database_window(db_store, db_event):
    for day in (1, 2, 3):
        db_store.submit(db_event | {"run_id": str(uuid4()), "started_at": f"2026-01-0{day}T00:00:00Z",
                                   "finished_at": f"2026-01-0{day}T00:00:01Z"})
    result = history(db_store.dsn, db_event["pipeline_id"], 2)
    assert result["window"] == {"requested_limit": 2, "returned": 2, "truncated": True}
    assert result["runs"][0]["started_at"].startswith("2026-01-02")
    assert result["runs"][0]["comparison"] is None
    assert result["successful_processing_groups"][0]["row_counts"]["input_count"]["known_total"] == 160
    assert history(db_store.dsn, "unknown")["execution_count"] == 0
    with pytest.raises(ValueError): history(db_store.dsn, db_event["pipeline_id"], 1001)
