"""Version 1 execution snapshots, validated at the monitoring boundary."""
import copy
import json
import math
import re
from datetime import UTC, datetime
from uuid import UUID


CONTRACT_VERSION = "1"
STATUSES = {"running", "completed", "completed_with_warnings", "failed", "reused"}
FIELDS = {
    "contract_version", "pipeline_id", "run_id", "batch_id", "status",
    "started_at", "finished_at", "duration_ms", "reused_from_run_id",
    "row_counts", "quality_metrics", "properties", "evidence", "error_summary",
}
MAX_INTEGER = 2**63 - 1


class EventValidationError(ValueError):
    """A report or monitoring event violates its declared contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EventValidationError(message)


def _text(value: object, field: str) -> None:
    _require(isinstance(value, str) and bool(value.strip()), f"{field}: expected nonempty text")


def _count(value: object, field: str) -> None:
    _require(value is None or (type(value) is int and 0 <= value <= MAX_INTEGER),
             f"{field}: expected null or a nonnegative 64-bit integer")


def _uuid(value: object, field: str) -> None:
    _text(value, field)
    try:
        canonical = str(UUID(value))
    except ValueError as error:
        raise EventValidationError(f"{field}: expected UUID") from error
    _require(value == canonical, f"{field}: expected canonical lowercase, hyphenated UUID")


def _timestamp(value: object, field: str) -> datetime:
    _text(value, field)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise EventValidationError(f"{field}: expected ISO 8601 timestamp") from error
    _require("T" in value and parsed.tzinfo is not None,
             f"{field}: expected timestamp with T and explicit timezone")
    return parsed.astimezone(UTC)


def quality_metric(count: int | None, denominator: int | None) -> dict:
    """Build a row-quality measurement; unknown or zero denominators have no rate."""
    _count(count, "quality.count")
    _count(denominator, "quality.denominator")
    if count is not None and denominator is not None:
        _require(count <= denominator, "quality.count: cannot exceed denominator")
    rate = count / denominator if count is not None and denominator else None
    return {"count": count, "denominator": denominator, "rate": rate}


def _named_mapping(value: object, field: str) -> None:
    _require(isinstance(value, dict), f"{field}: expected object")
    for name in value:
        _require(isinstance(name, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name) is not None,
                 f"{field}: names must be lowercase snake_case, at most 64 characters")


def validate_event(value: object) -> dict:
    """Return a detached, UTC-normalized event; reject coercions and unknown fields."""
    _require(isinstance(value, dict), "event: expected object")
    missing, extra = FIELDS - value.keys(), value.keys() - FIELDS
    _require(not missing and not extra,
             f"event fields: missing={sorted(missing)}, unknown={sorted(map(str, extra))}")
    event = copy.deepcopy(value)
    _require(event["contract_version"] == CONTRACT_VERSION, "contract_version: unsupported version")
    pipeline_id = event["pipeline_id"]
    _require(isinstance(pipeline_id, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", pipeline_id) is not None,
             "pipeline_id: expected identifier of 1–128 letters, digits, underscores, dots or hyphens")
    _uuid(event["run_id"], "run_id")
    # Batch labels are opaque; a failed execution may have received an empty label.
    _require(event["batch_id"] is None or isinstance(event["batch_id"], str), "batch_id: expected text or null")
    status = event["status"]
    _require(isinstance(status, str) and status in STATUSES, "status: unsupported execution status")
    started = _timestamp(event["started_at"], "started_at")
    event["started_at"] = started.isoformat()
    _count(event["duration_ms"], "duration_ms")
    if status == "running":
        _require(event["finished_at"] is None and event["duration_ms"] is None,
                 "running: finished_at and duration_ms must be null")
    else:
        finished = _timestamp(event["finished_at"], "finished_at")
        _require(finished >= started, "finished_at: precedes started_at")
        _require(event["duration_ms"] is not None, "duration_ms: required for terminal executions")
        event["finished_at"] = finished.isoformat()
    error = event["error_summary"]
    if status == "failed":
        _text(error, "error_summary")
    else:
        _require(error is None, "error_summary: must be null unless status is failed")
    previous = event["reused_from_run_id"]
    if status == "reused":
        _uuid(previous, "reused_from_run_id")
        _require(previous != event["run_id"], "reused_from_run_id: cannot reference itself")
    else:
        _require(previous is None, "reused_from_run_id: only allowed for reused executions")
    _named_mapping(event["row_counts"], "row_counts")
    for name, count in event["row_counts"].items():
        _count(count, f"row_counts.{name}")
    _named_mapping(event["quality_metrics"], "quality_metrics")
    for name, measurement in event["quality_metrics"].items():
        field = f"quality_metrics.{name}"
        _require(isinstance(measurement, dict) and measurement.keys() == {"count", "denominator", "rate"},
                 f"{field}: expected count, denominator and rate")
        expected = quality_metric(measurement["count"], measurement["denominator"])
        rate = measurement["rate"]
        if expected["rate"] is None:
            _require(rate is None, f"{field}.rate: must be null for unknown counts or zero/unknown denominator")
        else:
            _require(type(rate) in (int, float) and 0 <= rate <= 1 and math.isfinite(rate),
                     f"{field}.rate: expected finite number between 0 and 1")
            _require(math.isclose(rate, expected["rate"], rel_tol=1e-12, abs_tol=1e-12),
                     f"{field}.rate: disagrees with count / denominator")
        event["quality_metrics"][name] = expected
    for field in ("properties", "evidence"):
        _named_mapping(event[field], field)
        for name, text in event[field].items():
            _text(text, f"{field}.{name}")
    return event


def loads_json(text: str) -> object:
    """Read strict JSON without silently accepting duplicate keys or NaN/Infinity."""
    def reject_constant(value):
        raise EventValidationError(f"JSON: non-finite constant {value}")

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            _require(key not in result, f"JSON: duplicate key {key}")
            result[key] = value
        return result

    return json.loads(text, parse_constant=reject_constant, object_pairs_hook=unique_object)


def dumps_event(event: dict) -> str:
    """Serialize deterministically so redelivery of a report has identical content."""
    return json.dumps(validate_event(event), indent=2, sort_keys=True, allow_nan=False) + "\n"
