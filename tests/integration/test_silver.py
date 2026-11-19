"""Check retail policies, deterministic survivors, and persisted reconciliation."""

import csv
import hashlib
import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DecimalType, IntegerType, TimestampType

from pipelines.bronze import SOURCE_SCHEMA, ingest_csv
from pipelines.quality import classify_transactions, split_classified
from pipelines.silver import inspect_output, process_bronze
from pipelines.spark import create_local_spark

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = create_local_spark("pipelineiq-silver-tests")
    try:
        yield session
    finally:
        session.stop()


def records(changes: list[dict]) -> list[dict]:
    return [dict(
        transaction_id=f"tx-{index}", customer_id="customer-1", product_id="product-1",
        quantity="2", unit_price="10.00", transaction_timestamp="2026-01-01T10:00:00+00:00",
        source_file="fixture.csv", batch_id="fixture",
    ) | change for index, change in enumerate(changes)]


def frame(spark: SparkSession, changes: list[dict]) -> DataFrame:
    return spark.createDataFrame(records(changes), SOURCE_SCHEMA)


def metadata(path: Path) -> dict:
    actions = [json.loads(line) for line in (path / "_delta_log" / "00000000000000000000.json").read_text().splitlines()]
    return json.loads(next(action["commitInfo"]["userMetadata"] for action in actions if "commitInfo" in action))


def test_missing_fields_and_overlapping_reasons(spark: SparkSession) -> None:
    classified = classify_transactions(frame(spark, [
        {"customer_id": None},
        {"product_id": " \t "},
        {"transaction_timestamp": ""},
        {"transaction_timestamp": "2026-02-30T12:00:00Z"},
        {"transaction_id": None, "customer_id": None, "product_id": "", "quantity": "0",
         "unit_price": "-1", "transaction_timestamp": None},
        {"transaction_id": "\t"},
    ]))
    actual = {row.transaction_id: set(row.reason_codes) for row in classified.collect()}
    assert actual["tx-0"] == {"MISSING_CUSTOMER_ID"}
    assert actual["tx-1"] == {"MISSING_PRODUCT_ID"}
    assert actual["tx-2"] == {"MISSING_TRANSACTION_TIMESTAMP"}
    assert actual["tx-3"] == {"INVALID_TRANSACTION_TIMESTAMP"}
    assert actual[None] == {
        "MISSING_TRANSACTION_ID", "MISSING_CUSTOMER_ID", "MISSING_PRODUCT_ID",
        "MISSING_TRANSACTION_TIMESTAMP", "INVALID_QUANTITY", "INVALID_UNIT_PRICE",
    }
    assert actual["\t"] == {"MISSING_TRANSACTION_ID"}  # Missing keys are not duplicate groups.


def test_bad_numbers_do_not_crash_or_round_into_valid_data(spark: SparkSession) -> None:
    bad_quantities = [None, "", "0", "-1", "1.2", "abc", "2147483648", "1e3"]
    bad_prices = [None, "", "-0.01", "1.001", "NaN", "Infinity", "10000000000000000.00", "1e2"]
    changes = [{"quantity": value} for value in bad_quantities] + [{"unit_price": value} for value in bad_prices]
    rows = classify_transactions(frame(spark, changes)).collect()
    reasons = {row.transaction_id: row.reason_codes for row in rows}
    for index in range(len(changes)):
        assert reasons[f"tx-{index}"] == ["INVALID_QUANTITY" if index < len(bad_quantities) else "INVALID_UNIT_PRICE"]
    assert next(row for row in rows if row.unit_price == "1.001").parsed_unit_price is None


def test_types_normalization_zero_price_and_explicit_timezone(spark: SparkSession) -> None:
    classified = classify_transactions(frame(spark, [
        {"transaction_id": " tx ", "customer_id": "\tcustomer\t", "product_id": " product ",
         "quantity": " +0002 ", "unit_price": " 001.20 ", "transaction_timestamp": "2026-01-01T05:30:00.123456+05:30"},
        {"unit_price": "0.00"},
        {"transaction_timestamp": "2026-01-01T10:00:00"},
        {"transaction_timestamp": "now"},
    ]))
    silver, quarantine = split_classified(classified)
    assert isinstance(silver.schema["quantity"].dataType, IntegerType)
    assert silver.schema["unit_price"].dataType == DecimalType(18, 2)
    assert isinstance(silver.schema["transaction_timestamp"].dataType, TimestampType)
    rows = {row.transaction_id: row for row in silver.collect()}
    assert set(rows) == {"tx", "tx-1"}
    assert rows["tx"].customer_id == "customer"
    assert rows["tx"].product_id == "product"
    assert rows["tx"].quantity == 2
    assert rows["tx"].unit_price == Decimal("1.20")
    assert rows["tx-1"].unit_price == Decimal("0.00")
    utc = silver.where(F.col("transaction_id") == "tx").select(F.date_format("transaction_timestamp", "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()[0]
    assert utc == "2026-01-01 00:00:00.123456"
    assert all(row.reason_codes == ["INVALID_TRANSACTION_TIMESTAMP"] for row in quarantine.collect())


def test_duplicate_survivors_are_stable_and_prefer_valid_rows(spark: SparkSession) -> None:
    changes = [
        {"transaction_id": "dup", "customer_id": None, "transaction_timestamp": "2026-01-01T08:00:00Z"},
        {"transaction_id": " dup ", "customer_id": "later", "transaction_timestamp": "2026-01-01T12:00:00Z"},
        {"transaction_id": "dup", "customer_id": "winner", "transaction_timestamp": "2026-01-01T09:00:00Z"},
        {"transaction_id": "tie", "customer_id": "z"},
        {"transaction_id": "tie", "customer_id": "a"},
        {"transaction_id": "identical"}, {"transaction_id": "identical"},
        {"transaction_id": "all-bad", "quantity": "0", "transaction_timestamp": "2026-01-01T08:00:00Z"},
        {"transaction_id": "all-bad", "unit_price": "-1"},
    ]
    snapshots = []
    for changes_in_order, partitions in ((changes, 1), (list(reversed(changes)), 3)):
        classified = classify_transactions(frame(spark, changes_in_order).repartition(partitions)).cache()
        try:
            silver, quarantine = split_classified(classified)
            valid = {row.transaction_id: row.customer_id for row in silver.collect()}
            assert valid == {"dup": "winner", "tie": "a", "identical": "customer-1"}
            rejected = quarantine.collect()
            assert len(rejected) == 6
            assert sum("DUPLICATE_TRANSACTION_ID" in row.reason_codes for row in rejected) == 5
            assert next(row for row in rejected if row.customer_id is None).reason_codes == ["MISSING_CUSTOMER_ID", "DUPLICATE_TRANSACTION_ID"]
            snapshots.append(Counter((row.source_record_hash, tuple(row.reason_codes)) for row in classified.collect()))
        finally:
            classified.unpersist()
    assert snapshots[0] == snapshots[1]


@pytest.mark.parametrize("scenario", ["mixed", "healthy", "empty", "all-invalid"])
def test_saved_outputs_reconcile_and_retain_evidence(
    spark: SparkSession, tmp_path: Path, scenario: str, capsys: pytest.CaptureFixture,
) -> None:
    cases = {
        "mixed": ([{}, {"transaction_id": "dup"}, {"transaction_id": "dup", "customer_id": None},
                   {"transaction_id": "dup", "transaction_timestamp": "2026-01-01T11:00:00Z"},
                   {"quantity": "0"}, {"customer_id": None, "unit_price": "-1"},
                   {"transaction_id": None}, {"transaction_id": " trim ", "unit_price": "001.20"}], 3, 5, 2),
        "healthy": ([{}, {}], 2, 0, 0),
        "empty": ([], 0, 0, 0),
        "all-invalid": ([{"customer_id": None}, {"quantity": "abc"}], 0, 2, 0),
    }
    changes, valid_count, rejected_count, duplicate_count = cases[scenario]
    original = records(changes)
    source = tmp_path / "fixture.csv"
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=SOURCE_SCHEMA.fieldNames())
        writer.writeheader()
        writer.writerows(original)
    bronze = ingest_csv(spark, source, "fixture", tmp_path / "bronze")
    result = process_bronze(spark, Path(bronze.bronze_path), tmp_path / "silver", tmp_path / "quarantine")
    silver = spark.read.format("delta").load(result.silver_path)
    quarantine = spark.read.format("delta").load(result.quarantine_path)
    valid_rows, rejected_rows = silver.collect(), quarantine.collect()
    assert len(valid_rows) == result.valid_count == valid_count
    assert len(rejected_rows) == result.quarantined_count == rejected_count
    assert result.duplicate_count == duplicate_count
    assert result.input_count == valid_count + rejected_count == len(original)
    assert len({row.transaction_id for row in valid_rows}) == valid_count
    expected_hashes = Counter(hashlib.sha256(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest() for row in original)
    assert Counter(row.source_record_hash for row in valid_rows + rejected_rows) == expected_hashes
    raw_records = {tuple(row[name] for name in SOURCE_SCHEMA.fieldNames()) for row in original}
    assert all(tuple(row[name] for name in SOURCE_SCHEMA.fieldNames()) in raw_records for row in rejected_rows)
    assert all(row.reason_codes for row in rejected_rows)
    assert all(row.validation_run_id == result.validation_run_id and row.run_id == bronze.run_id for row in valid_rows + rejected_rows)
    assert result.bronze_version == 0
    assert metadata(Path(result.silver_path)) == metadata(Path(result.quarantine_path)) == asdict(result)
    assert not (Path(bronze.bronze_path) / "_delta_log" / "00000000000000000001.json").exists()
    with pytest.raises(FileExistsError, match="Output already exists"):
        process_bronze(spark, Path(bronze.bronze_path), tmp_path / "silver", tmp_path / "quarantine")
    # A collision in quarantine must be noticed before creating a new Silver table.
    with pytest.raises(FileExistsError):
        process_bronze(spark, Path(bronze.bronze_path), tmp_path / "new-silver", tmp_path / "quarantine")
    assert not (tmp_path / "new-silver").exists()
    with pytest.raises(ValueError, match="overlap"):
        process_bronze(spark, Path(bronze.bronze_path), tmp_path / "same", tmp_path / "same")
    if scenario == "mixed":
        inspect_output(spark, Path(result.quarantine_path), limit=2)
        assert '"stored_row_count": 5' in capsys.readouterr().out


def test_missing_schema_and_non_delta_input_fail_clearly(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Missing source columns"):
        classify_transactions(frame(spark, [{}]).drop("quantity"))
    with pytest.raises(ValueError, match="Not a Bronze Delta table"):
        process_bronze(spark, tmp_path)
