"""Validate a Bronze batch and persist Silver and quarantine Delta tables."""

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

from pipelines.bronze import SOURCE_SCHEMA
from pipelines.quality import classify_transactions, split_classified
from pipelines.spark import create_local_spark


@dataclass(frozen=True)
class SilverResult:
    validation_run_id: str
    batch_id: str
    bronze_path: str
    bronze_version: int
    silver_path: str
    quarantine_path: str
    input_count: int
    valid_count: int
    quarantined_count: int
    duplicate_count: int
    validated_at: str


def process_bronze(
    spark: SparkSession,
    bronze_path: Path,
    silver_root: Path = Path("data/silver"),
    quarantine_root: Path = Path("data/quarantine"),
) -> SilverResult:
    """Process a fixed Bronze version; reject either existing output destination.

    Two independent Delta writes are not one atomic transaction. A failure after
    the first write can leave a partial pair; never silently overwrite it on retry.
    """
    bronze_path = bronze_path.resolve(strict=True)
    if not DeltaTable.isDeltaTable(spark, str(bronze_path)):
        raise ValueError(f"Not a Bronze Delta table: {bronze_path}")
    commit = DeltaTable.forPath(spark, str(bronze_path)).history(1).first()
    provenance = json.loads(commit["userMetadata"] or "{}")
    batch_id = provenance.get("batch_id", "")
    if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", batch_id):
        raise ValueError("Bronze commit must contain a valid ingestion batch_id")
    silver_path = silver_root.resolve() / batch_id
    quarantine_path = quarantine_root.resolve() / batch_id
    destinations = (silver_path, quarantine_path)
    if silver_path == quarantine_path or any(
        left in right.parents or right in left.parents
        for left, right in ((silver_path, quarantine_path), (silver_path, bronze_path), (quarantine_path, bronze_path))
    ):
        raise ValueError("Bronze, Silver, and quarantine table paths must not overlap")
    for path in destinations:
        if path.exists():
            raise FileExistsError(f"Output already exists: {path}; inspect it or use different output roots")

    version = commit["version"]
    raw = spark.read.format("delta").option("versionAsOf", version).load(str(bronze_path))
    required = set(SOURCE_SCHEMA.fieldNames()) | {"run_id", "ingestion_batch_id", "ingested_at", "ingestion_source_path"}
    if not required.issubset(raw.columns):
        raise ValueError("Expected a Bronze table with source columns and A3 ingestion metadata")
    validation_run_id = str(uuid4())
    validated_at = datetime.now(UTC)
    classified = (
        classify_transactions(raw)
        .withColumn("validation_run_id", F.lit(validation_run_id))
        .withColumn("validated_at", F.lit(validated_at))
        .cache()
    )
    try:
        totals = classified.agg(
            F.count("*").alias("input_count"),
            F.sum(F.when(F.size("reason_codes") == 0, 1).otherwise(0)).alias("valid_count"),
            F.sum(F.when(F.size("reason_codes") > 0, 1).otherwise(0)).alias("quarantined_count"),
            F.sum(F.when(F.array_contains("reason_codes", "DUPLICATE_TRANSACTION_ID"), 1).otherwise(0)).alias("duplicate_count"),
        ).first()
        result = SilverResult(
            validation_run_id=validation_run_id,
            batch_id=batch_id,
            bronze_path=str(bronze_path),
            bronze_version=version,
            silver_path=str(silver_path),
            quarantine_path=str(quarantine_path),
            input_count=totals.input_count,
            valid_count=totals.valid_count or 0,
            quarantined_count=totals.quarantined_count or 0,
            duplicate_count=totals.duplicate_count or 0,
            validated_at=validated_at.isoformat(),
        )
        if result.input_count != result.valid_count + result.quarantined_count:
            raise ValueError("Silver/quarantine row reconciliation failed")
        if result.input_count != provenance.get("input_count"):
            raise ValueError("Bronze row count differs from its ingestion receipt")
        silver, quarantine = split_classified(classified)
        for frame, destination in ((silver, silver_path), (quarantine, quarantine_path)):
            (
                frame.write.format("delta").mode("errorifexists")
                .option("userMetadata", json.dumps(asdict(result)))
                .save(str(destination))
            )
    finally:
        classified.unpersist()
    return result


def inspect_output(spark: SparkSession, table_path: Path, limit: int = 5) -> None:
    """Show the stage receipt, stored row count, and business columns/reasons."""
    if limit < 1:
        raise ValueError("limit must be at least 1")
    table_path = table_path.resolve(strict=True)
    if not DeltaTable.isDeltaTable(spark, str(table_path)):
        raise ValueError(f"Not a Delta table: {table_path}")
    stored = spark.read.format("delta").load(str(table_path))
    commit = DeltaTable.forPath(spark, str(table_path)).history(1).first()
    report = json.loads(commit["userMetadata"] or "{}")
    report["stored_row_count"] = stored.count()
    print(json.dumps(report, indent=2))
    stored.printSchema()
    columns = SOURCE_SCHEMA.fieldNames()
    if "reason_codes" in stored.columns:
        columns.append("reason_codes")
    stored.select(*columns).orderBy("transaction_id").show(limit, truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    process = commands.add_parser("process", help="Validate a Bronze table into Silver and quarantine")
    process.add_argument("--bronze-table", type=Path, required=True)
    process.add_argument("--silver-root", type=Path, default=Path("data/silver"))
    process.add_argument("--quarantine-root", type=Path, default=Path("data/quarantine"))
    inspect = commands.add_parser("inspect", help="Read a Silver or quarantine table")
    inspect.add_argument("--table", type=Path, required=True)
    inspect.add_argument("--limit", type=int, default=5)
    options = parser.parse_args()
    spark = create_local_spark("pipelineiq-silver")
    try:
        if options.command == "process":
            result = process_bronze(spark, options.bronze_table, options.silver_root, options.quarantine_root)
            print(json.dumps(asdict(result), indent=2))
        else:
            inspect_output(spark, options.table, options.limit)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
