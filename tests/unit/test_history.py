"""History totals have explicit coverage, version boundaries and replay semantics."""
from copy import deepcopy
from uuid import uuid4

import pytest

pytest.importorskip("psycopg")
from monitoring.history import compare, markdown


def another(event, **changes):
    copy = deepcopy(event)
    copy.update(run_id=str(uuid4()), **changes)
    return copy


def test_reused_and_failed_do_not_inflate_totals(db_event):
    later = another(db_event, started_at="2026-01-02T00:00:00Z", finished_at="2026-01-02T00:00:01Z")
    later["row_counts"]["input_count"] = 100
    later["quality_metrics"]["missing_id"] = {"count": 1, "denominator": 100, "rate": .01}
    reused = another(later, status="reused", reused_from_run_id=later["run_id"])
    failed = another(later, status="failed", error_summary="failed")
    result = compare([failed, later, db_event, reused], db_event["pipeline_id"])
    group = result["successful_processing_groups"][0]
    assert group["row_counts"]["input_count"]["known_total"] == 180
    assert group["quality_metrics"]["missing_id"]["rate"] == 12/180
    row = next(row for row in result["runs"] if row["run_id"] == later["run_id"])
    assert row["comparison"]["row_count_deltas"]["input_count"] == 20
    assert row["comparison"]["quality_rate_deltas"]["missing_id"] == pytest.approx(-.1275)
    assert result["status_counts"]["reused"] == 1


def test_unknown_empty_and_version_changes(db_event):
    first = deepcopy(db_event)
    first["row_counts"] = {"input_count": None}
    first["quality_metrics"] = {"empty": {"count": 0, "denominator": 0, "rate": None}}
    second = another(first, properties={"policy_version": "new"})
    result = compare([first, second], first["pipeline_id"])
    assert len(result["successful_processing_groups"]) == 2
    for group in result["successful_processing_groups"]:
        assert group["row_counts"]["input_count"] == {"known_total": None, "known_runs": 0, "unknown_runs": 1}
        assert group["quality_metrics"]["empty"]["rate"] is None
    assert all(row["comparison"] is None for row in result["runs"])


def test_empty_history_and_invalid_identities(db_event):
    assert compare([], "empty")["execution_count"] == 0
    with pytest.raises(ValueError, match="one pipeline"): compare([db_event], "other")
    with pytest.raises(ValueError, match="duplicate"): compare([db_event, db_event], db_event["pipeline_id"])


def test_markdown_escapes_producer_text(db_event):
    db_event["batch_id"] = "<script>|\nexample"
    rendered = markdown(compare([db_event], db_event["pipeline_id"]))
    assert "<script>" not in rendered
    assert "&lt;script&gt;\\| example" in rendered
