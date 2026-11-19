"""Verify source preservation by reading the committed Delta tables back."""

import csv
import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path
from uuid import UUID

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType

from pipelines.bronze import SOURCE_SCHEMA, ingest_csv, inspect_bronze
from pipelines.generate import CSV_FIELDS, GenerationConfig, write_batch
from pipelines.spark import create_local_spark

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = create_local_spark("pipelineiq-bronze-tests")
    try:
        yield session
    finally:
        session.stop()


def read_commit_metadata(table: Path) -> dict:
    log = table / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    commit = next(action["commitInfo"] for action in actions if "commitInfo" in action)
    return json.loads(commit["userMetadata"])


@pytest.mark.parametrize("config", [
    GenerationConfig(batch_id="healthy", rows=20),
    GenerationConfig(
        batch_id="incident", rows=20, drop_rows=5, duplicate_ids=3,
        null_customer_ids=4, null_product_ids=2, invalid_quantities=3, invalid_prices=2,
    ),
], ids=["healthy", "incident"])
def test_ingestion_preserves_every_source_record_and_metadata(
    spark: SparkSession, tmp_path: Path, config: GenerationConfig,
) -> None:
    source = write_batch(config, tmp_path / "source")
    original_bytes = source.read_bytes()
    with source.open(newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        next(reader)
        expected = Counter(tuple(value if value != "" else None for value in row) for row in reader)

    result = ingest_csv(spark, source, config.batch_id, tmp_path / "bronze")
    stored = spark.read.format("delta").load(result.bronze_path)
    actual = Counter(tuple(row) for row in stored.select(*CSV_FIELDS).collect())
    assert actual == expected  # Counter retains multiplicity; a set would hide row loss.
    assert result.input_count == sum(expected.values())
    assert UUID(result.run_id).version == 4
    assert all(isinstance(stored.schema[name].dataType, StringType) for name in CSV_FIELDS)
    assert isinstance(stored.schema["ingested_at"].dataType, TimestampType)
    metadata = stored.select("run_id", "ingestion_batch_id", "ingestion_source_path", "ingested_at").distinct().collect()
    assert len(metadata) == 1
    assert metadata[0].run_id == result.run_id
    assert metadata[0].ingestion_batch_id == config.batch_id
    assert metadata[0].ingestion_source_path == str(source.resolve())
    assert metadata[0].ingested_at is not None
    assert read_commit_metadata(Path(result.bronze_path)) == asdict(result)
    assert source.read_bytes() == original_bytes

    # A rerun must neither append rows nor create another Delta commit.
    with pytest.raises(FileExistsError, match="already exists"):
        ingest_csv(spark, source, config.batch_id, tmp_path / "bronze")
    assert not (Path(result.bronze_path) / "_delta_log" / "00000000000000000001.json").exists()
    assert Counter(tuple(row) for row in stored.select(*CSV_FIELDS).collect()) == expected


def test_raw_text_quotes_and_identical_duplicates_survive(spark: SparkSession, tmp_path: Path) -> None:
    source = tmp_path / "raw.csv"
    record = [
        "same-id", None, '  product "quoted",\nsecond line  ', "abc", "001.2300",
        "not-a-timestamp", "source-as-reported.csv", "source-batch-as-reported",
    ]
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(CSV_FIELDS)
        writer.writerows([record, record])
    result = ingest_csv(spark, source, "raw", tmp_path / "bronze")
    stored = spark.read.format("delta").load(result.bronze_path)
    assert [tuple(row) for row in stored.select(*CSV_FIELDS).collect()] == [tuple(record)] * 2
    assert result.input_count == 2


def test_header_only_batch_keeps_schema_and_commit_metadata(
    spark: SparkSession, tmp_path: Path, capsys: pytest.CaptureFixture,
) -> None:
    source = write_batch(GenerationConfig(batch_id="empty", rows=0), tmp_path / "source")
    result = ingest_csv(spark, source, "empty", tmp_path / "bronze")
    stored = spark.read.format("delta").load(result.bronze_path)
    assert stored.count() == 0
    assert stored.select(*CSV_FIELDS).schema == SOURCE_SCHEMA
    assert read_commit_metadata(Path(result.bronze_path)) == asdict(result)
    assert result.input_count == 0
    inspect_bronze(spark, Path(result.bronze_path))
    output = capsys.readouterr().out
    assert '"stored_row_count": 0' in output
    assert result.run_id in output


@pytest.mark.parametrize("body", [
    "",
    "customer_id,transaction_id,product_id,quantity,unit_price,transaction_timestamp,source_file,batch_id\n",
    ",".join(CSV_FIELDS) + "\na,b,c,1,2,date,source\n",
    ",".join(CSV_FIELDS) + "\na,b,c,1,2,date,source,batch,extra\n",
    ",".join(CSV_FIELDS) + "\n\n",
    ",".join(CSV_FIELDS) + '\n"unterminated\n',
], ids=["no-header", "wrong-header", "missing-field", "extra-field", "blank-record", "broken-quote"])
def test_structural_errors_do_not_create_a_table(spark: SparkSession, tmp_path: Path, body: str) -> None:
    source = tmp_path / "invalid.csv"
    source.write_text(body, encoding="utf-8")
    with pytest.raises(ValueError):
        ingest_csv(spark, source, "invalid", tmp_path / "bronze")
    assert not (tmp_path / "bronze").exists()


def test_bad_paths_and_batch_ids_are_rejected(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        ingest_csv(spark, tmp_path / "missing.csv", "missing", tmp_path / "bronze")
    with pytest.raises(ValueError, match="one CSV file"):
        ingest_csv(spark, tmp_path, "directory", tmp_path / "bronze")
    with pytest.raises(ValueError, match="batch_id"):
        ingest_csv(spark, tmp_path / "missing.csv", "../escape", tmp_path / "bronze")
    occupied = tmp_path / "bronze" / "occupied"
    occupied.mkdir(parents=True)
    with pytest.raises(FileExistsError):
        ingest_csv(spark, tmp_path / "missing.csv", "occupied", tmp_path / "bronze")
