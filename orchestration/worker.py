"""Process a batch, retain attempt evidence, publish events, and verify outcomes."""
import argparse
import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from monitoring.events import dumps_event, loads_json, validate_event
from pipelines.monitoring import adapt_report
from pipelines.replay import batch_lock


def save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def scope_path(root: Path, run_key: str) -> Path:
    if not run_key:
        raise ValueError("An orchestration run key is required")
    return root / hashlib.sha256(run_key.encode()).hexdigest()


def process(source: Path, batch_id: str, pipeline_root: Path, scope: Path, run_key: str, attempt: int) -> int:
    if attempt < 1:
        raise ValueError("Attempt must be positive")
    directory = scope / f"attempt-{attempt:06d}"
    configuration = {"source": str(source.resolve()), "batch_id": batch_id,
                     "pipeline_root": str(pipeline_root.resolve())}
    with batch_lock(directory / "worker.lock"):
        receipt_path = directory / "receipt.json"
        if receipt_path.exists():
            receipt = loads_json(receipt_path.read_text())
            if receipt["configuration"] != configuration:
                raise ValueError("Attempt already recorded with different configuration; use a new run key")
            return receipt["exit_code"]
        # A killed worker may have a report but no receipt. Use a fresh execution UUID;
        # A6 protects published batch outputs. The interrupted report remains evidence.
        execution_id = str(uuid4())
        started = datetime.now(UTC).isoformat()
        clock = time.perf_counter()
        report_path = pipeline_root.resolve() / "metrics" / f"{execution_id}.json"
        spark = None
        error = None
        try:
            from pipelines.run import run_pipeline
            from pipelines.spark import create_local_spark
            spark = create_local_spark("pipelineiq-airflow")
            run_pipeline(spark, source, batch_id, pipeline_root, run_id=execution_id)
        except Exception as failure:
            error = failure
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception as stop_error:
                    print(f"Spark shutdown warning: {type(stop_error).__name__}")
        if report_path.exists():
            event = adapt_report(loads_json(report_path.read_text()), report_path)
        else:
            event = validate_event({
                "contract_version": "1", "pipeline_id": "retail_transactions", "run_id": execution_id,
                "batch_id": batch_id, "status": "failed", "started_at": started,
                "finished_at": datetime.now(UTC).isoformat(), "duration_ms": round((time.perf_counter()-clock)*1000),
                "reused_from_run_id": None, "row_counts": {"input_count": None, "valid_count": None, "quarantined_count": None},
                "quality_metrics": {}, "properties": {"failure_stage": "worker_startup"},
                "evidence": {"source": str(source.resolve())},
                "error_summary": f"{type(error).__name__}: {str(error)[:2000]}",
            })
        event["properties"].update(orchestration_run_id=run_key, orchestration_attempt=str(attempt))
        event_path = directory / "event.json"
        save(event_path, loads_json(dumps_event(event)))
        code = 1 if error is not None or event["status"] == "failed" else 0
        save(receipt_path, {"exit_code": code, "run_id": execution_id, "status": event["status"],
                            "configuration": configuration})
        print(json.dumps({"event": str(event_path), "status": event["status"], "exit_code": code}))
        return code


def publish(scope: Path, store) -> int:
    paths = sorted(scope.glob("attempt-*/event.json"))
    if not paths:
        raise ValueError("No attempt event exists; processing may have died before writing evidence")
    for path in paths:
        event = loads_json(path.read_text())
        outcome = store.submit(event)
        # A lost acknowledgement is safe: retry resubmits the identical event.
        save(path.with_name("published.json"), {"run_id": event["run_id"], "outcome": outcome})
    return len(paths)


def verify(scope: Path) -> None:
    receipts = sorted(scope.glob("attempt-*/receipt.json"))
    if not receipts or loads_json(receipts[-1].read_text())["exit_code"] != 0:
        raise ValueError("Latest processing attempt failed or has no receipt")
    for path in scope.glob("attempt-*/event.json"):
        published = path.with_name("published.json")
        if not published.exists() or loads_json(published.read_text())["run_id"] != loads_json(path.read_text())["run_id"]:
            raise ValueError("An attempt event has not been acknowledged by monitoring")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("process", "publish", "verify"))
    args = parser.parse_args()
    run_key = os.environ["PIPELINEIQ_ORCHESTRATION_RUN"]
    scope = scope_path(Path(os.environ.get("PIPELINEIQ_ORCHESTRATION_ROOT", "data/orchestration")), run_key)
    if args.command == "process":
        raise SystemExit(process(Path(os.environ["PIPELINEIQ_SOURCE"]), os.environ["PIPELINEIQ_BATCH_ID"],
                                 Path(os.environ.get("PIPELINEIQ_PIPELINE_ROOT", "data/pipeline")), scope,
                                 run_key, int(os.environ.get("PIPELINEIQ_ATTEMPT", "1"))))
    if args.command == "publish":
        from monitoring.store import EventStore, database_url
        print(json.dumps({"published_events": publish(scope, EventStore(database_url()))}))
    else:
        verify(scope)


if __name__ == "__main__":
    main()
