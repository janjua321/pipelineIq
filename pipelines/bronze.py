"""Preserve one retail CSV batch in a local Bronze Delta table."""

import argparse
import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipelines.spark import create_local_spark


# Bronze keeps source text, including values that cannot be cast to numbers/dates.
SOURCE_SCHEMA = StructType([
    StructField(name, StringType(), nullable=True)
    for name in (
        "transaction_id", "customer_id", "product_id", "quantity", "unit_price",
        "transaction_timestamp", "source_file", "batch_id",
    )
])


@dataclass(frozen=True)
class IngestionResult:
    run_id: str
    batch_id: str
    source_path: str
    bronze_path: str
    input_count: int
    ingested_at: str


def _check_csv_structure(source: Path) -> int:
    """Check header/record widths, without applying transaction-quality rules.

    Spark CSV parsing can pad missing fields or discard extra ones. A streaming
    preflight rejects those structural problems before any Delta write.
    """
    if not source.is_file():
        raise ValueError(f"Expected one CSV file: {source}")
    try:
        with source.open(newline="", encoding="utf-8") as stream:
            reader = csv.reader(stream, strict=True)
            if next(reader, None) != SOURCE_SCHEMA.fieldNames():
                raise ValueError("CSV header must match the eight source columns in order")
            count = 0
            for record in reader:
                if len(record) != len(SOURCE_SCHEMA):
                    raise ValueError(
                        f"CSV record ending at line {reader.line_num} has {len(record)} "
                        f"fields; expected {len(SOURCE_SCHEMA)}"
                    )
                count += 1
    except csv.Error as error:
        raise ValueError(f"Malformed CSV: {error}") from error
    return count


def ingest_csv(
    spark: SparkSession,
    source: Path,
    batch_id: str,
    bronze_root: Path = Path("data/bronze"),
) -> IngestionResult:
    """Write a new table for a batch, preserving all source rows and duplicates.

    The input file must remain unchanged during ingestion. Existing destinations
    are rejected; replay/merge behavior will be designed separately in A6.
    """
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", batch_id):
        raise ValueError("batch_id must be 1–64 letters/digits/hyphens/underscores, starting with a letter or digit")
    destination = bronze_root.resolve() / batch_id
    if destination.exists():
        raise FileExistsError(
            f"Bronze destination already exists: {destination}. "
            "Use inspect to view it, or a different --output-dir for a separate copy."
        )
    source = source.resolve(strict=True)
    expected_count = _check_csv_structure(source)
    run_id = str(uuid4())
    ingested_at = datetime.now(UTC)

    raw = (
        spark.read.schema(SOURCE_SCHEMA)
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .option("multiLine", True)
        .option("escape", '"')
        .option("ignoreLeadingWhiteSpace", False)
        .option("ignoreTrailingWhiteSpace", False)
        .csv(str(source))
    )
    bronze = (
        raw.withColumn("run_id", F.lit(run_id))
        .withColumn("ingestion_batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.lit(ingested_at))
        .withColumn("ingestion_source_path", F.lit(str(source)))
        .cache()
    )
    try:
        # This action materializes the cached rows before counting and writing.
        actual_count = bronze.count()
        if actual_count != expected_count:
            raise ValueError(
                f"CSV row reconciliation failed: {expected_count} source records, "
                f"{actual_count} Spark rows"
            )
        result = IngestionResult(
            run_id=run_id,
            batch_id=batch_id,
            source_path=str(source),
            bronze_path=str(destination),
            input_count=actual_count,
            ingested_at=ingested_at.isoformat(),
        )
        (
            bronze.write.format("delta")
            .mode("errorifexists")
            # Commit metadata also preserves provenance for header-only batches.
            .option("userMetadata", json.dumps(asdict(result)))
            .save(str(destination))
        )
    finally:
        bronze.unpersist()
    return result


def inspect_bronze(spark: SparkSession, table_path: Path, limit: int = 5) -> None:
    """Print persisted ingestion metadata, actual row count, schema, and a sample."""
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
    stored.orderBy("transaction_id").show(limit, truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest", help="Save one source CSV as a new Bronze table")
    ingest.add_argument("--source", type=Path, required=True)
    ingest.add_argument("--batch-id", required=True)
    ingest.add_argument("--output-dir", type=Path, default=Path("data/bronze"))
    inspect = commands.add_parser("inspect", help="Read an existing Bronze Delta table")
    inspect.add_argument("--table", type=Path, required=True)
    inspect.add_argument("--limit", type=int, default=5)
    options = parser.parse_args()

    spark = create_local_spark("pipelineiq-bronze")
    try:
        if options.command == "ingest":
            result = ingest_csv(spark, options.source, options.batch_id, options.output_dir)
            print(json.dumps(asdict(result), indent=2))
        else:
            inspect_bronze(spark, options.table, options.limit)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
