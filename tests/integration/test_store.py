"""Persistence and concurrency verified against real PostgreSQL."""
import copy
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

pytest.importorskip("psycopg")

from monitoring import store as module
from monitoring.store import EventConflict, connect, migrate

pytestmark = pytest.mark.postgres


def running(event):
    return event | {"status": "running", "finished_at": None, "duration_ms": None,
                    "row_counts": {"input_count": None}, "quality_metrics": {}}


def test_migration_repeat_and_concurrent(db_store):
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(migrate, [db_store.dsn] * 2)) == [[], []]
    with connect(db_store.dsn) as conn:
        assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 1


def test_changed_applied_migration_rejected(db_store, tmp_path, monkeypatch):
    directory = tmp_path / "migrations"
    directory.mkdir()
    (directory / "001_executions.sql").write_text("SELECT 1;")
    monkeypatch.setattr(module, "files", lambda package: tmp_path)
    with pytest.raises(ValueError, match="Applied migration changed"):
        migrate(db_store.dsn)


def test_roundtrip_metrics_and_duplicate(db_store, db_event):
    assert db_store.submit(db_event) == "created"
    assert db_store.submit(db_event) == "unchanged"
    assert db_store.get(db_event["pipeline_id"], db_event["run_id"]) == db_event
    with connect(db_store.dsn) as conn:
        values = conn.execute("SELECT count, denominator, rate FROM quality_metrics WHERE pipeline_id=%s",
                              (db_event["pipeline_id"],)).fetchall()
        assert values == [(11, 80, 0.1375)]
        assert conn.execute("SELECT count(*) FROM executions WHERE pipeline_id=%s",
                            (db_event["pipeline_id"],)).fetchone()[0] == 1


def test_lifecycle_conflicts_and_nulls(db_store, db_event):
    initial = running(db_event)
    assert db_store.submit(initial) == "created"
    changed = initial | {"row_counts": {"input_count": 80}}
    with pytest.raises(EventConflict):
        db_store.submit(changed)
    with pytest.raises(EventConflict, match="cannot change"):
        db_store.submit(db_event | {"batch_id": "other"})
    assert db_store.submit(db_event) == "updated"
    assert db_store.submit(db_event) == "unchanged"
    with pytest.raises(EventConflict):
        db_store.submit(initial)
    with pytest.raises(EventConflict):
        db_store.submit(db_event | {"duration_ms": 2000})
    assert db_store.get(db_event["pipeline_id"], db_event["run_id"]) == db_event


def test_failed_null_and_empty_denominator_persist(db_store, db_event):
    failed = running(db_event) | {"status": "failed", "finished_at": db_event["finished_at"],
                                  "duration_ms": 10, "error_summary": "source missing"}
    failed["quality_metrics"] = {"unknown": {"count": None, "denominator": None, "rate": None},
                                  "empty": {"count": 0, "denominator": 0, "rate": None}}
    db_store.submit(failed)
    with connect(db_store.dsn) as conn:
        assert conn.execute("SELECT count FROM row_counts WHERE pipeline_id=%s",
                            (db_event["pipeline_id"],)).fetchone() == (None,)
        assert conn.execute("SELECT count(*) FROM quality_metrics WHERE pipeline_id=%s AND rate IS NULL",
                            (db_event["pipeline_id"],)).fetchone()[0] == 2


def test_concurrent_identical_submissions(db_store, db_event):
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(db_store.submit, [db_event] * 4))
    assert sorted(outcomes) == ["created", "unchanged", "unchanged", "unchanged"]


def test_concurrent_conflicting_terminal_updates(db_store, db_event):
    db_store.submit(running(db_event))
    def submit(event):
        try:
            return db_store.submit(event)
        except EventConflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(submit, [db_event, db_event | {"duration_ms": 2000}]))
    assert sorted(outcomes) == ["conflict", "updated"]


def test_metric_failure_rolls_back_execution_and_children(db_store, db_event, monkeypatch):
    original = running(db_event)
    db_store.submit(original)
    def fail(conn, event):
        conn.execute("DELETE FROM row_counts WHERE pipeline_id=%s", (event["pipeline_id"],))
        raise RuntimeError("injected metric write failure")
    monkeypatch.setattr(module, "_write_metrics", fail)
    with pytest.raises(RuntimeError, match="injected"):
        db_store.submit(db_event)
    assert db_store.get(db_event["pipeline_id"], db_event["run_id"]) == original
    with connect(db_store.dsn) as conn:
        assert conn.execute("SELECT count FROM row_counts WHERE pipeline_id=%s", (db_event["pipeline_id"],)).fetchone() == (None,)


def test_filter_paging_and_reused_reference(db_store, db_event):
    db_store.submit(db_event)
    reused = copy.deepcopy(db_event)
    reused.update(run_id=str(uuid4()), status="reused", reused_from_run_id=db_event["run_id"])
    db_store.submit(reused)
    assert db_store.list(pipeline_id=db_event["pipeline_id"], status="reused") == [reused]
    page1 = db_store.list(pipeline_id=db_event["pipeline_id"], limit=1)
    page2 = db_store.list(pipeline_id=db_event["pipeline_id"], limit=1, offset=1)
    assert page1[0]["run_id"] != page2[0]["run_id"]
    assert db_store.list(pipeline_id="' OR 1=1 --") == []
    assert db_store.get(db_event["pipeline_id"], str(uuid4())) is None
    with pytest.raises(ValueError):
        db_store.list(limit=101)
