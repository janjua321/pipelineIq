"""Generic events accept other pipelines and reject ambiguous measurements."""
import copy
import json

import pytest

from monitoring.events import EventValidationError, dumps_event, loads_json, quality_metric, validate_event


@pytest.fixture
def event():
    # This pipeline expands four documents into eight records; retail rules do not apply.
    return {
        "contract_version": "1", "pipeline_id": "document_index", "batch_id": None,
        "run_id": "11111111-1111-4111-8111-111111111111", "status": "completed",
        "started_at": "2026-01-01T05:30:00+05:30", "finished_at": "2026-01-01T00:00:01Z",
        "duration_ms": 1000, "reused_from_run_id": None, "error_summary": None,
        "row_counts": {"input_count": 4, "output_count": 8},
        "quality_metrics": {"unreadable_documents": {"count": 1, "denominator": 4, "rate": 0.25}},
        "properties": {"parser_version": "v2"}, "evidence": {"log": "logs/indexer.txt"},
    }


def test_generic_pipeline_round_trip_and_utc_normalization(event):
    before = copy.deepcopy(event)
    validated = validate_event(event)
    assert validated["started_at"] == "2026-01-01T00:00:00+00:00"
    assert validated["row_counts"] == {"input_count": 4, "output_count": 8}
    assert loads_json(dumps_event(event)) == validated
    assert dumps_event(loads_json(dumps_event(event))) == dumps_event(event)
    validated["quality_metrics"]["unreadable_documents"]["count"] = 0
    assert event == before  # Validation does not alias or mutate the input.


@pytest.mark.parametrize("count,denominator,rate", [(0, 0, None), (None, 80, None), (3, None, None), (0, 80, 0), (11, 80, 0.1375)])
def test_quality_denominators(count, denominator, rate):
    assert quality_metric(count, denominator) == {"count": count, "denominator": denominator, "rate": rate}


@pytest.mark.parametrize("field,value,match", [
    ("contract_version", "2", "contract_version"),
    ("run_id", "not-a-uuid", "run_id"),
    ("run_id", "11111111111141118111111111111111", "canonical"),
    ("pipeline_id", "../outside", "pipeline_id"),
    ("status", "success", "status"),
    ("status", [], "status"),
    ("started_at", "2026-01-01T00:00:00", "timezone"),
    ("finished_at", "2025-12-31T23:59:59Z", "precedes"),
    ("finished_at", None, "finished_at"),
    ("duration_ms", True, "duration_ms"),
    ("duration_ms", -1, "duration_ms"),
    ("duration_ms", None, "duration_ms"),
    ("row_counts", {"input_count": "4"}, "row_counts.input_count"),
    ("row_counts", {"input_count": 1.0}, "row_counts.input_count"),
    ("row_counts", {"input_count": True}, "row_counts.input_count"),
    ("row_counts", {"input_count": 2**63}, "64-bit"),
    ("properties", {"parser_version": 1}, "properties.parser_version"),
    ("evidence", {"log": ""}, "evidence.log"),
    ("quality_metrics", {"bad name": {}}, "snake_case"),
    ("error_summary", "failed", "error_summary"),
    ("reused_from_run_id", "22222222-2222-4222-8222-222222222222", "only allowed"),
])
def test_invalid_envelope(event, field, value, match):
    event[field] = value
    with pytest.raises(EventValidationError, match=match):
        validate_event(event)


@pytest.mark.parametrize("measurement", [
    {"count": 1, "denominator": 4},
    {"count": 1, "denominator": 4, "rate": 0.5},
    {"count": 1, "denominator": 4, "rate": "0.25"},
    {"count": 1, "denominator": 1, "rate": True},
    {"count": 1, "denominator": 4, "rate": float("nan")},
    {"count": 1, "denominator": 4, "rate": float("inf")},
    {"count": -1, "denominator": 4, "rate": -0.25},
    {"count": 5, "denominator": 4, "rate": 1.25},
    {"count": 0, "denominator": 0, "rate": 0},
    {"count": None, "denominator": 4, "rate": 0},
    {"count": 1, "denominator": None, "rate": 0},
])
def test_reject_ambiguous_quality_measurements(event, measurement):
    event["quality_metrics"] = {"unreadable_documents": measurement}
    with pytest.raises(EventValidationError):
        validate_event(event)


def test_lifecycle_and_reuse_reference(event):
    event.update(status="reused", reused_from_run_id="22222222-2222-4222-8222-222222222222")
    assert validate_event(event)["status"] == "reused"
    event["reused_from_run_id"] = event["run_id"]
    with pytest.raises(EventValidationError, match="itself"):
        validate_event(event)
    event.update(status="failed", reused_from_run_id=None)
    with pytest.raises(EventValidationError, match="error_summary"):
        validate_event(event)
    event["error_summary"] = "Source unavailable"
    assert validate_event(event)["status"] == "failed"
    event.update(status="running", error_summary=None)
    with pytest.raises(EventValidationError, match="running"):
        validate_event(event)
    event.update(finished_at=None, duration_ms=None)
    assert validate_event(event)["finished_at"] is None


def test_exact_top_level_contract(event):
    event["typo"] = 1
    with pytest.raises(EventValidationError, match="unknown=.*typo"):
        validate_event(event)
    del event["typo"]
    del event["contract_version"]
    with pytest.raises(EventValidationError, match="missing=.*contract_version"):
        validate_event(event)


@pytest.mark.parametrize("text", ['{"a": 1, "a": 2}', '{"x": {"a": 1, "a": 2}}', '{"rate": NaN}', '{"rate": Infinity}'])
def test_strict_json(text):
    with pytest.raises(EventValidationError):
        loads_json(text)


def test_malformed_json():
    with pytest.raises(json.JSONDecodeError):
        loads_json('{"broken"')
