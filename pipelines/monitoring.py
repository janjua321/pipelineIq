"""Adapt retail run-report JSON into a generic monitoring execution snapshot."""
import argparse
import math
import re
from pathlib import Path
from uuid import uuid4

from monitoring.events import (
    CONTRACT_VERSION, EventValidationError, dumps_event, loads_json, quality_metric, validate_event,
)


ROW_COUNTS = ("input_count", "valid_count", "quarantined_count")
QUALITY_COUNTS = ("quarantined_count", "duplicate_count", "null_customer_id_count", "invalid_amount_count")
REQUIRED_REPORT_FIELDS = {
    "run_id", "pipeline_id", "batch_id", "started_at", "finished_at", "duration_ms", "status",
    "schema_version", "policy_version", "source_path", "source_sha256", "error_summary",
    *ROW_COUNTS, *QUALITY_COUNTS,
}
SUCCESS_STATUSES = ("completed", "completed_with_warnings", "reused")


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise EventValidationError(f"retail report: {message}")


def adapt_report(report: object, report_path: Path | None = None) -> dict:
    """Translate schema 1 reports; enforce retail semantics outside the monitor."""
    _check(isinstance(report, dict), "expected object")
    _check(REQUIRED_REPORT_FIELDS <= report.keys(),
           f"missing fields {sorted(REQUIRED_REPORT_FIELDS - report.keys())}")
    _check(report["pipeline_id"] == "retail_transactions", "unsupported pipeline_id")
    _check(report["schema_version"] == "1", "unsupported schema_version")
    properties = {"producer_schema_version": report["schema_version"], "policy_version": report["policy_version"]}
    source_hash = report["source_sha256"]
    if source_hash is not None:
        _check(isinstance(source_hash, str) and re.fullmatch(r"[0-9a-f]{64}", source_hash) is not None,
               "source_sha256 must be a lowercase SHA-256 digest")
        properties["source_sha256"] = source_hash
    revenue = report.get("total_revenue")
    if revenue is not None:
        _check(isinstance(revenue, str) and re.fullmatch(r"[0-9]+\.[0-9]{2}", revenue) is not None,
               "total_revenue must be nonnegative decimal text with two places")
        properties["total_revenue"] = revenue
    evidence = {"source": report["source_path"]}
    if report_path is not None:
        evidence["report"] = str(report_path.resolve())
    outputs = report.get("outputs", {})
    _check(isinstance(outputs, dict) and outputs.keys() <= {"bronze", "silver", "quarantine", "gold"},
           "unexpected output table names")
    evidence.update(outputs)
    event = validate_event({
        "contract_version": CONTRACT_VERSION,
        **{name: report[name] for name in ("pipeline_id", "run_id", "batch_id", "status", "started_at",
                                          "finished_at", "duration_ms", "error_summary")},
        "reused_from_run_id": report.get("reused_from_run_id"),
        "row_counts": {name: report[name] for name in ROW_COUNTS},
        "quality_metrics": {name: quality_metric(report[name], report["input_count"]) for name in QUALITY_COUNTS},
        "properties": properties,
        "evidence": evidence,
    })
    counts = event["row_counts"]
    if all(value is not None for value in counts.values()):
        _check(counts["input_count"] == counts["valid_count"] + counts["quarantined_count"],
               "input_count must equal valid_count + quarantined_count")
    for name in QUALITY_COUNTS:
        if report[name] is not None and counts["quarantined_count"] is not None:
            _check(report[name] <= counts["quarantined_count"], f"{name} exceeds quarantined_count")
    for name in ("valid_count", "quarantined_count"):
        if counts[name] is not None and counts["input_count"] is not None:
            _check(counts[name] <= counts["input_count"], f"{name} exceeds input_count")
    if report["status"] in SUCCESS_STATUSES:
        _check(all(report[name] is not None for name in (*ROW_COUNTS, *QUALITY_COUNTS)),
               "successful/reused executions require all counts")
        _check(source_hash is not None and revenue is not None and len(outputs) == 4,
               "successful/reused executions require source hash, revenue and all output paths")
        _check("rates" in report, "successful/reused executions require rates")
        if report["status"] != "reused":
            _check((report["status"] == "completed_with_warnings") == (counts["quarantined_count"] > 0),
                   "completed status disagrees with quarantined_count")
    if "rates" in report:
        rates = report["rates"]
        _check(isinstance(rates, dict) and rates.keys() == set(QUALITY_COUNTS), "unexpected rate names")
        for name, rate in rates.items():
            expected = event["quality_metrics"][name]["rate"]
            matches = rate is None if expected is None else (
                type(rate) in (int, float) and 0 <= rate <= 1 and math.isfinite(rate)
                and math.isclose(rate, expected, rel_tol=1e-12, abs_tol=1e-12)
            )
            _check(matches, f"rates.{name} disagrees with measured count / input_count")
    return event


def export_report(report_path: Path, output_dir: Path) -> tuple[dict, Path]:
    """Atomically export the current snapshot at its stable pipeline/run identity."""
    event = adapt_report(loads_json(report_path.read_text(encoding="utf-8")), report_path)
    serialized = dumps_event(event)
    destination = output_dir / event["pipeline_id"] / f"{event['run_id']}.json"
    _check(destination.resolve() != report_path.resolve(), "export must not overwrite its source report")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.read_text(encoding="utf-8") == serialized:
        return event, destination
    temporary = destination.with_name(f".{destination.name}.{uuid4()}.tmp")
    try:
        temporary.write_text(serialized, encoding="utf-8")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return event, destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True, help="A data/pipeline/metrics/<run-id>.json report")
    parser.add_argument("--output-dir", type=Path, default=Path("data/monitoring/events"))
    args = parser.parse_args()
    try:
        event, _ = export_report(args.report, args.output_dir)
    except (OSError, ValueError) as error:
        parser.exit(2, f"Monitoring export failed: {error}\n")
    print(dumps_event(event), end="")


if __name__ == "__main__":
    main()
