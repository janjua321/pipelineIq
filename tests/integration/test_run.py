"""Exact Gold totals and complete persisted run evidence."""
import json
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

from pipelines.gold import aggregate_daily
from pipelines.generate import GenerationConfig, write_batch
from pipelines.run import run_pipeline
from pipelines.spark import create_local_spark

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    session = create_local_spark("pipelineiq-run-tests")
    yield session
    session.stop()


def test_gold_exact_daily_totals_and_empty_schema(spark):
    schema = StructType([StructField("product_id", StringType()), StructField("quantity", IntegerType()),
                         StructField("unit_price", DecimalType(18, 2)), StructField("transaction_timestamp", TimestampType())])
    source = spark.createDataFrame([
        ("p1", 3, Decimal("0.10"), datetime(2026, 1, 1, 12)),
        ("p1", 2, Decimal("1.25"), datetime(2026, 1, 1, 13)),
        ("p1", 1, Decimal("5.00"), datetime(2026, 1, 2, 12)),
        ("p2", 1, Decimal("0.00"), datetime(2026, 1, 1, 12)),
    ], schema)
    rows = {(str(row.transaction_date), row.product_id): row for row in aggregate_daily(source).collect()}
    assert rows[("2026-01-01", "p1")].revenue == Decimal("2.80")
    assert rows[("2026-01-01", "p1")].transaction_count == 2
    assert rows[("2026-01-01", "p1")].units_sold == 5
    assert rows[("2026-01-02", "p1")].revenue == Decimal("5.00")
    assert rows[("2026-01-01", "p2")].revenue == 0
    empty = aggregate_daily(spark.createDataFrame([], schema))
    assert empty.count() == 0
    assert empty.schema == aggregate_daily(source).schema


@pytest.mark.parametrize("size", [0, 6])
def test_run_report_is_persisted_and_reconciled(spark, tmp_path, size):
    source = write_batch(GenerationConfig(batch_id="run", rows=size, null_customer_ids=size,
                                         invalid_quantities=size, invalid_prices=size), tmp_path / "source")
    report = run_pipeline(spark, source, "run", tmp_path / "pipeline")
    assert report["input_count"] == report["quarantined_count"] == size
    assert report["valid_count"] == 0
    assert report["invalid_amount_count"] == size  # A row with both errors counts once.
    assert report["null_customer_id_count"] == size
    assert report["rates"]["invalid_amount_count"] == (1 if size else None)
    assert report["total_revenue"] == "0.00"
    assert report["status"] == ("completed_with_warnings" if size else "completed")
    saved = json.loads((tmp_path / "pipeline" / "metrics" / f"{report['run_id']}.json").read_text())
    assert saved == report
    assert report["duration_ms"] >= 0 and report["finished_at"] >= report["started_at"]
    assert spark.read.format("delta").load(report["outputs"]["gold"]).count() == 0


def test_failure_report_keeps_unknown_counts_null(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        run_pipeline(spark, tmp_path / "absent.csv", "missing", tmp_path / "pipeline")
    report = json.loads(next((tmp_path / "pipeline" / "metrics").glob('*.json')).read_text())
    assert report["status"] == "failed"
    assert report["input_count"] is None
    assert "FileNotFoundError" in report["error_summary"]
    assert report["finished_at"] is not None
