"""Check source-data properties independently of the future Spark pipeline."""

import csv
import random
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from pipelines.generate import GenerationConfig, generate_transactions, write_batch


def test_healthy_batch_has_valid_fields_and_unique_business_keys() -> None:
    rows = generate_transactions(GenerationConfig(batch_id="healthy", rows=100))
    assert len(rows) == 100
    assert len({row["transaction_id"] for row in rows}) == 100
    for row in rows:
        assert row["transaction_id"] and row["customer_id"] and row["product_id"]
        assert row["quantity"] > 0
        assert Decimal(row["unit_price"]) >= 0
        assert Decimal(row["unit_price"]).as_tuple().exponent == -2
        timestamp = datetime.fromisoformat(row["transaction_timestamp"])
        assert datetime(2026, 1, 1, tzinfo=UTC) <= timestamp < datetime(2026, 1, 2, tzinfo=UTC)
        assert row["source_file"] == "healthy.csv"
        assert row["batch_id"] == "healthy"


def test_seed_repeats_values_without_changing_global_random_state() -> None:
    config = GenerationConfig(batch_id="repeat", seed=42)
    state = random.getstate()
    original = generate_transactions(config)
    assert generate_transactions(config) == original
    assert generate_transactions(replace(config, seed=43)) != original
    assert random.getstate() == state


def test_business_keys_differ_between_batches() -> None:
    first = generate_transactions(GenerationConfig(batch_id="batch-a"))
    second = generate_transactions(GenerationConfig(batch_id="batch-b"))
    assert {row["transaction_id"] for row in first}.isdisjoint(
        row["transaction_id"] for row in second
    )


def test_csv_preserves_exact_defect_counts_after_volume_reduction(tmp_path: Path) -> None:
    path = write_batch(GenerationConfig(
        batch_id="incident", rows=100, drop_rows=20, duplicate_ids=7,
        null_customer_ids=11, null_product_ids=3, invalid_quantities=5, invalid_prices=9,
    ), tmp_path)
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == [
            "transaction_id", "customer_id", "product_id", "quantity", "unit_price",
            "transaction_timestamp", "source_file", "batch_id",
        ]
        rows = list(reader)
    assert len(rows) == 80
    assert len(rows) - len({row["transaction_id"] for row in rows}) == 7
    assert sum(row["customer_id"] == "" for row in rows) == 11
    assert sum(row["product_id"] == "" for row in rows) == 3
    assert sum(int(row["quantity"]) <= 0 for row in rows) == 5
    assert sum(Decimal(row["unit_price"]) < 0 for row in rows) == 9


def test_categories_can_overlap_and_duplicate_count_means_excess_rows() -> None:
    rows = generate_transactions(GenerationConfig(
        batch_id="overlap", rows=4, duplicate_ids=3, null_customer_ids=4,
        null_product_ids=4, invalid_quantities=4, invalid_prices=4,
    ))
    assert len(rows) == 4
    assert len({row["transaction_id"] for row in rows}) == 1
    assert all(
        row["customer_id"] is None and row["product_id"] is None
        and row["quantity"] == 0 and Decimal(row["unit_price"]) < 0
        for row in rows
    )


def test_volume_reduction_keeps_a_prefix_of_the_healthy_batch() -> None:
    config = GenerationConfig(batch_id="volume", rows=10)
    assert generate_transactions(replace(config, drop_rows=7)) == generate_transactions(config)[:3]


@pytest.mark.parametrize("rows,drop_rows", [(0, 0), (10, 10)])
def test_empty_batch_writes_only_a_header(tmp_path: Path, rows: int, drop_rows: int) -> None:
    path = write_batch(GenerationConfig(batch_id="empty", rows=rows, drop_rows=drop_rows), tmp_path)
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        assert list(reader) == []
        assert reader.fieldnames is not None
        assert "transaction_id" in reader.fieldnames


def test_repeated_write_is_unchanged_and_conflict_preserves_original(tmp_path: Path) -> None:
    config = GenerationConfig(batch_id="replay", null_customer_ids=5, duplicate_ids=3)
    path = write_batch(config, tmp_path / "first")
    original = path.read_bytes()
    timestamp = path.stat().st_mtime_ns
    assert write_batch(config, tmp_path / "first") == path
    assert path.read_bytes() == original
    assert path.stat().st_mtime_ns == timestamp
    assert write_batch(config, tmp_path / "second").read_bytes() == original
    with pytest.raises(FileExistsError, match="different data"):
        write_batch(replace(config, seed=43), tmp_path / "first")
    assert path.read_bytes() == original


@pytest.mark.parametrize("changes", [
    {"batch_id": "../escape"}, {"batch_id": ""}, {"rows": -1},
    {"drop_rows": -1}, {"rows": 2, "drop_rows": 3},
    {"duplicate_ids": -1}, {"rows": 2, "duplicate_ids": 2},
    {"rows": 0, "duplicate_ids": 1},
    {"null_customer_ids": -1}, {"null_product_ids": -1},
    {"invalid_quantities": -1}, {"invalid_prices": -1},
    {"rows": 2, "drop_rows": 1, "null_customer_ids": 2},
    {"rows": 2, "null_product_ids": 3},
    {"rows": 2, "invalid_quantities": 3}, {"rows": 2, "invalid_prices": 3},
])
def test_invalid_requests_do_not_create_output(tmp_path: Path, changes: dict) -> None:
    config = replace(GenerationConfig(batch_id="invalid"), **changes)
    with pytest.raises(ValueError):
        write_batch(config, tmp_path / "source")
    assert not (tmp_path / "source").exists()


def test_command_line_writes_source_and_explains_invalid_requests(tmp_path: Path) -> None:
    command = [
        sys.executable, "-m", "pipelines.generate", "--batch-id", "cli",
        "--rows", "5", "--drop-rows", "1", "--null-customer-ids", "2",
        "--output-dir", str(tmp_path),
    ]
    success = subprocess.run(command, capture_output=True, text=True, check=False)
    assert success.returncode == 0, success.stderr
    assert "Source batch ready:" in success.stdout
    path = tmp_path / "cli.csv"
    original = path.read_bytes()
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 4
    assert sum(row["customer_id"] == "" for row in rows) == 2

    failure = subprocess.run(command + ["--duplicate-ids", "4"], capture_output=True, text=True, check=False)
    assert failure.returncode == 2
    assert "duplicate_ids" in failure.stderr
    assert "Traceback" not in failure.stderr
    assert path.read_bytes() == original
