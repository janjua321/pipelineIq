"""Run source → Bronze → Silver/quarantine → Gold and persist measured metrics."""
import argparse
import hashlib
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F

from pipelines.bronze import ingest_csv
from pipelines.gold import aggregate_daily
from pipelines.silver import process_bronze
from pipelines.spark import create_local_spark
from pipelines.replay import batch_lock, digest, output_digests, verify_manifest


SCHEMA_VERSION = "1"
POLICY_VERSION = "retail-v1"


def save_json(path: Path, value: dict) -> None:
    """Publish a complete JSON document using a same-directory atomic rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def execute_attempt(spark: SparkSession, source: Path, batch_id: str, root: Path, report: dict) -> None:
    """Build isolated outputs; incomplete attempts never modify existing batches."""
    attempt = root / "attempts" / report["run_id"]
    attempt.mkdir(parents=True)
    content = source.read_bytes()
    report["source_sha256"] = hashlib.sha256(content).hexdigest()
    snapshot = attempt / "source" / source.name
    snapshot.parent.mkdir()
    snapshot.write_bytes(content)
    bronze = ingest_csv(spark, snapshot, batch_id, attempt / "bronze")
    report["input_count"] = bronze.input_count
    result = process_bronze(spark, Path(bronze.bronze_path), attempt / "silver", attempt / "quarantine")
    report.update(input_count=result.input_count, valid_count=result.valid_count,
                  quarantined_count=result.quarantined_count, duplicate_count=result.duplicate_count)
    silver = spark.read.format("delta").load(result.silver_path)
    quarantine = spark.read.format("delta").load(result.quarantine_path)
    quality = quarantine.agg(
        F.sum(F.when(F.array_contains("reason_codes", "MISSING_CUSTOMER_ID"), 1).otherwise(0)).alias("missing_customer"),
        F.sum(F.when(F.array_contains("reason_codes", "INVALID_QUANTITY") |
                     F.array_contains("reason_codes", "INVALID_UNIT_PRICE"), 1).otherwise(0)).alias("invalid_amount"),
    ).first()
    # Missing includes null/empty/whitespace IDs, matching the validation policy.
    report["null_customer_id_count"] = quality.missing_customer or 0
    report["invalid_amount_count"] = quality.invalid_amount or 0
    denominator = result.input_count
    report["rates"] = {name: report[name] / denominator if denominator else None for name in (
        "quarantined_count", "duplicate_count", "null_customer_id_count", "invalid_amount_count"
    )}
    gold = aggregate_daily(silver).cache()
    try:
        totals = gold.agg(F.sum("transaction_count").alias("transactions"), F.sum("revenue").alias("revenue")).first()
        if (totals.transactions or 0) != result.valid_count:
            raise ValueError("Gold transaction count does not reconcile with Silver")
        gold_path = attempt / "gold" / batch_id
        gold.write.format("delta").mode("errorifexists").save(str(gold_path))
        report["total_revenue"] = str(totals.revenue) if totals.revenue is not None else "0.00"
        report["outputs"] = {"bronze": bronze.bronze_path, "silver": result.silver_path,
                             "quarantine": result.quarantine_path, "gold": str(gold_path)}
    finally:
        gold.unpersist()
    report["status"] = "completed_with_warnings" if result.quarantined_count else "completed"


def run_pipeline(spark: SparkSession, source: Path, batch_id: str, root: Path = Path("data/pipeline")) -> dict:
    root = root.resolve()
    started = time.perf_counter()
    report = dict(run_id=str(uuid4()), pipeline_id="retail_transactions", batch_id=batch_id,
                  started_at=datetime.now(UTC).isoformat(), finished_at=None, duration_ms=None,
                  status="running", schema_version=SCHEMA_VERSION, policy_version=POLICY_VERSION,
                  source_path=str(source.resolve()), source_sha256=None,
                  input_count=None, valid_count=None, quarantined_count=None, duplicate_count=None,
                  null_customer_id_count=None, invalid_amount_count=None, error_summary=None)
    report_path = root / "metrics" / f"{report['run_id']}.json"
    save_json(report_path, report)
    try:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", batch_id):
            raise ValueError("Invalid batch_id")
        with batch_lock(root / "locks" / f"{batch_id}.lock"):
            source = source.resolve(strict=True)
            source_digest = digest(source)
            report["source_sha256"] = source_digest
            manifest_path = root / "batches" / f"{batch_id}.json"
            if manifest_path.exists():
                previous = json.loads(manifest_path.read_text())
                verify_manifest(previous, source_digest, POLICY_VERSION)
                for name in ("input_count", "valid_count", "quarantined_count", "duplicate_count",
                             "null_customer_id_count", "invalid_amount_count", "rates", "outputs", "total_revenue"):
                    report[name] = previous[name]
                report["status"] = "reused"
                report["reused_from_run_id"] = previous["run_id"]
            else:
                execute_attempt(spark, source, batch_id, root, report)
                if report["source_sha256"] != source_digest:
                    raise ValueError("Source changed before snapshot; retry with a stable source")
                report["finished_at"] = datetime.now(UTC).isoformat()
                report["duration_ms"] = round((time.perf_counter() - started) * 1000)
                save_json(report_path, report)
                manifest = report | {"artifacts": output_digests(report["outputs"])}
                save_json(manifest_path, manifest)
    except Exception as error:
        report["status"] = "failed"
        report["error_summary"] = f"{type(error).__name__}: {str(error)[:2000]}"
        raise
    finally:
        if report["finished_at"] is None or report["status"] == "failed":
            report["finished_at"] = datetime.now(UTC).isoformat()
            report["duration_ms"] = round((time.perf_counter() - started) * 1000)
        save_json(report_path, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--root", type=Path, default=Path("data/pipeline"))
    parser.add_argument("--show-gold", action="store_true", help="Display up to 20 daily product aggregates")
    args = parser.parse_args()
    spark = create_local_spark("pipelineiq-run")
    try:
        report = run_pipeline(spark, args.source, args.batch_id, args.root)
        print(json.dumps(report, indent=2))
        if args.show_gold:
            spark.read.format("delta").load(report["outputs"]["gold"]).orderBy("transaction_date", "product_id").show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
