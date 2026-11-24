"""Durable attempt evidence, failure publication and final DAG verdict."""
import json

import pytest

from orchestration.worker import process, publish, save, scope_path, verify


def test_scope_key_cannot_escape_root(tmp_path):
    assert scope_path(tmp_path, "../../elsewhere").parent == tmp_path
    assert scope_path(tmp_path, "run1") != scope_path(tmp_path, "run2")


def test_startup_failure_retains_event_and_fails_verdict(tmp_path, monkeypatch):
    import pipelines.spark
    def fail(name):
        raise RuntimeError("JVM unavailable")
    monkeypatch.setattr(pipelines.spark, "create_local_spark", fail)
    scope = tmp_path / "scope"
    assert process(tmp_path / "source.csv", "batch", tmp_path / "pipeline", scope, "run", 1) == 1
    event_path = scope / "attempt-000001" / "event.json"
    event = json.loads(event_path.read_text())
    assert event["status"] == "failed"
    assert event["row_counts"]["input_count"] is None
    assert event["properties"]["failure_stage"] == "worker_startup"
    assert "JVM unavailable" in event["error_summary"]
    before = event_path.read_bytes()
    assert process(tmp_path / "source.csv", "batch", tmp_path / "pipeline", scope, "run", 1) == 1
    assert event_path.read_bytes() == before
    with pytest.raises(ValueError, match="different configuration"):
        process(tmp_path / "other.csv", "batch", tmp_path / "pipeline", scope, "run", 1)
    with pytest.raises(ValueError, match="Latest processing"):
        verify(scope)


def test_delivery_retry_and_missing_acknowledgement(tmp_path, db_event):
    scope = tmp_path / "scope"
    path = scope / "attempt-000001"
    save(path / "event.json", db_event)
    save(path / "receipt.json", {"exit_code": 0})
    class Store:
        def __init__(self): self.events = []
        def submit(self, event):
            self.events.append(event)
            if len(self.events) == 1: raise ConnectionError("lost acknowledgement")
            return "unchanged"
    store = Store()
    with pytest.raises(ConnectionError): publish(scope, store)
    with pytest.raises(ValueError, match="acknowledged"): verify(scope)
    assert publish(scope, store) == 1
    assert store.events == [db_event, db_event]
    verify(scope)
    save(path / "receipt.json", {"exit_code": 1})
    with pytest.raises(ValueError): verify(scope)


def test_no_evidence_is_failure(tmp_path):
    with pytest.raises(ValueError): publish(tmp_path, None)
    with pytest.raises(ValueError): verify(tmp_path)
