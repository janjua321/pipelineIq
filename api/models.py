"""OpenAPI models delegate semantic validation to the B1 event contract."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, StrictInt, model_validator

from monitoring.events import validate_event


Status = Literal["running", "completed", "completed_with_warnings", "failed", "reused"]


class QualityMetric(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    count: StrictInt | None
    denominator: StrictInt | None
    rate: float | None


class ExecutionEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    contract_version: Literal["1"]
    pipeline_id: str
    run_id: str
    batch_id: str | None
    status: Status
    started_at: str
    finished_at: str | None
    duration_ms: StrictInt | None
    reused_from_run_id: str | None
    row_counts: dict[str, StrictInt | None]
    quality_metrics: dict[str, QualityMetric]
    properties: dict[str, str]
    evidence: dict[str, str]
    error_summary: str | None

    @model_validator(mode="before")
    @classmethod
    def enforce_contract(cls, value):
        if isinstance(value, cls):
            return value
        return validate_event(value)


class Submission(BaseModel):
    outcome: Literal["created", "updated", "unchanged"]
    pipeline_id: str
    run_id: str


class RunPage(BaseModel):
    items: list[ExecutionEvent]
    limit: int
    offset: int
