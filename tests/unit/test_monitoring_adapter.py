"""Retail translation, partial failures, and a CLI that does not load Spark."""
import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from monitoring.events import EventValidationError, loads_json
from pipelines.monitoring import adapt_report, export_report


@pytest.fixture
def report():
    # Illustrative test fixture; measured demonstration results live in PROGRESS.md.
    return {
        "run_id": "11111111-1111-4111-8111-111111111111", "pipeline_id": "retail_transactions",
        "batch_id": "incident", "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:00:01+00:00", "duration_ms": 1000,
        "status": "completed_with_warnings", "schema_version": "1", "policy_version": "retail-v1",
        "source_path": "/fixtures/incident.csv", "source_sha256": "a" * 64,
        "input_count": 10, "valid_count": 7, "quarantined_count": 3,
        "duplicate_count": 2, "null_customer_id_count": 2, "invalid_amount_count": 3,
        "rates": {"quarantined_count": 0.3, "duplicate_count": 0.2, "null_customer_id_count": 0.2, "invalid_amount_count": 0.3},
        "total_revenue": "123.45", "error_summary": None,
        "outputs": {name: f"/tables/{name}" for name in ("bronze", "silver", "quarantine", "gold")},
    }


def test_adapter_preserves_metrics_overlap_decimal_and_evidence(report, tmp_path):
    before = copy.deepcopy(report)
    event = adapt_report(report, tmp_path / "report.json")
    assert event["row_counts"] == {"input_count": 10, "valid_count": 7, "quarantined_count": 3}
    assert event["quality_metrics"]["invalid_amount_count"] == {"count": 3, "denominator": 10, "rate": 0.3}
    assert sum(event["quality_metrics"][name]["count"] for name in ("duplicate_count", "null_customer_id_count", "invalid_amount_count")) > 3
    assert event["properties"]["total_revenue"] == "123.45"
    assert event["properties"]["producer_schema_version"] == event["contract_version"] == "1"
    assert event["evidence"]["gold"] == "/tables/gold"
    assert event["evidence"]["report"] == str(tmp_path / "report.json")
    assert report == before


def test_empty_success_and_reused_execution(report):
    for name in ("input_count", "valid_count", *report["rates"]):
        report[name] = 0
    report.update(status="completed", total_revenue="0.00", rates={name: None for name in report["rates"]})
    event = adapt_report(report)
    assert all(metric == {"count": 0, "denominator": 0, "rate": None} for metric in event["quality_metrics"].values())
    report.update(status="reused", reused_from_run_id="22222222-2222-4222-8222-222222222222")
    reused = adapt_report(report)
    assert reused["status"] == "reused"
    assert reused["reused_from_run_id"] == report["reused_from_run_id"]
    assert reused["run_id"] == report["run_id"]
    assert reused["evidence"] == event["evidence"]


@pytest.mark.parametrize("input_count", [None, 10])
def test_running_and_failed_reports_preserve_unknowns(report, input_count):
    for name in ("valid_count", *report["rates"]):
        report[name] = None
    for field in ("rates", "total_revenue", "outputs"):
        del report[field]
    report.update(status="running", input_count=input_count, finished_at=None, duration_ms=None, source_sha256=None)
    assert adapt_report(report)["status"] == "running"
    report.update(status="failed", finished_at="2026-01-01T00:00:01Z", duration_ms=1000,
                  error_summary="OSError: cannot read source")
    event = adapt_report(report)
    assert event["row_counts"]["input_count"] == input_count
    assert event["row_counts"]["valid_count"] is None
    assert event["quality_metrics"]["invalid_amount_count"] == {"count": None, "denominator": input_count, "rate": None}
    assert event["error_summary"] == report["error_summary"]
    assert "total_revenue" not in event["properties"]
    assert "gold" not in event["evidence"]


@pytest.mark.parametrize("field,value,match", [
    ("pipeline_id", "document_index", "pipeline_id"),
    ("schema_version", "2", "schema_version"),
    ("source_sha256", "oops", "source_sha256"),
    ("total_revenue", 123.45, "total_revenue"),
    ("total_revenue", "NaN", "total_revenue"),
    ("valid_count", 6, "input_count must equal"),
    ("valid_count", None, "require all counts"),
    ("duplicate_count", 4, "exceeds quarantined_count"),
    ("invalid_amount_count", True, "integer"),
    ("outputs", {}, "all output paths"),
    ("rates", {}, "unexpected rate names"),
    ("status", "completed", "status disagrees"),
])
def test_retail_contract_rejections(report, field, value, match):
    report[field] = value
    with pytest.raises(EventValidationError, match=match):
        adapt_report(report)


def test_missing_fields_and_inconsistent_rate(report):
    report["rates"]["invalid_amount_count"] = 0.6
    with pytest.raises(EventValidationError, match="rates.invalid_amount_count"):
        adapt_report(report)
    del report["input_count"]
    with pytest.raises(EventValidationError, match="missing fields.*input_count"):
        adapt_report(report)


def test_export_is_deterministic_and_rejects_invalid_update(report, tmp_path):
    source = tmp_path / "report.json"
    source.write_text(json.dumps(report))
    event, destination = export_report(source, tmp_path / "events")
    original_bytes, original_mtime = destination.read_bytes(), destination.stat().st_mtime_ns
    assert loads_json(destination.read_text()) == event
    assert export_report(source, tmp_path / "events") == (event, destination)
    assert destination.stat().st_mtime_ns == original_mtime
    assert len(list((tmp_path / "events").rglob("*.json"))) == 1
    report["valid_count"] = -1
    source.write_text(json.dumps(report))
    with pytest.raises(EventValidationError):
        export_report(source, tmp_path / "events")
    assert destination.read_bytes() == original_bytes
    assert not list(destination.parent.glob("*.tmp"))


def test_export_updates_running_snapshot_and_preserves_on_write_failure(report, tmp_path, monkeypatch):
    source = tmp_path / "report.json"
    running = report | {"status": "running", "finished_at": None, "duration_ms": None}
    source.write_text(json.dumps(running))
    _, destination = export_report(source, tmp_path / "events")
    before = destination.read_bytes()
    source.write_text(json.dumps(report))
    with monkeypatch.context() as patch:
        def fail_replace(self, target):
            raise OSError("Injected publication failure")
        patch.setattr(Path, "replace", fail_replace)
        with pytest.raises(OSError, match="Injected"):
            export_report(source, tmp_path / "events")
    assert destination.read_bytes() == before
    assert not list(destination.parent.glob("*.tmp"))
    event, same_path = export_report(source, tmp_path / "events")
    assert event["status"] == "completed_with_warnings"
    assert same_path == destination


def test_cli_uses_standard_library_and_returns_useful_errors(report, tmp_path):
    source = tmp_path / "report.json"
    source.write_text(json.dumps(report))
    # -S disables site-packages entirely: PySpark, Delta and pytest are unavailable.
    command = [sys.executable, "-S", "-m", "pipelines.monitoring", "--report", str(source),
               "--output-dir", str(tmp_path / "events")]
    result = subprocess.run(command, cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert loads_json(result.stdout)["row_counts"]["input_count"] == 10
    source.write_text('{"bad": NaN}')
    failed = subprocess.run(command, cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, timeout=15)
    assert failed.returncode == 2
    assert "Monitoring export failed" in failed.stderr and "NaN" in failed.stderr
    assert "Traceback" not in failed.stderr
