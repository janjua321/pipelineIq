"""Generate small, reproducible retail CSV batches with controlled defects."""

import argparse
import csv
import io
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TypedDict


class Transaction(TypedDict):
    transaction_id: str
    customer_id: str | None
    product_id: str | None
    quantity: int
    unit_price: str
    transaction_timestamp: str
    source_file: str
    batch_id: str


CSV_FIELDS = (
    "transaction_id", "customer_id", "product_id", "quantity", "unit_price",
    "transaction_timestamp", "source_file", "batch_id",
)


@dataclass(frozen=True)
class GenerationConfig:
    batch_id: str
    rows: int = 100
    seed: int = 42
    drop_rows: int = 0
    duplicate_ids: int = 0
    null_customer_ids: int = 0
    null_product_ids: int = 0
    invalid_quantities: int = 0
    invalid_prices: int = 0

    def validate(self) -> None:
        """Reject impossible requests instead of silently clipping defect counts."""
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", self.batch_id):
            raise ValueError("batch_id must be 1–64 letters/digits/hyphens/underscores, starting with a letter or digit")
        if self.rows < 0 or not 0 <= self.drop_rows <= self.rows:
            raise ValueError("rows must be nonnegative and drop_rows must be between 0 and rows")
        remaining = self.rows - self.drop_rows
        if not 0 <= self.duplicate_ids <= max(remaining - 1, 0):
            raise ValueError("duplicate_ids must leave at least one distinct transaction ID in a nonempty batch")
        for name in ("null_customer_ids", "null_product_ids", "invalid_quantities", "invalid_prices"):
            if not 0 <= getattr(self, name) <= remaining:
                raise ValueError(f"{name} must be between 0 and {remaining} (rows after volume reduction)")


def generate_transactions(config: GenerationConfig) -> list[Transaction]:
    """Use a local RNG and a fixed UTC day; never use the current clock.

    Duplicate IDs replace keys in existing rows, without appending rows. Other
    defects are sampled independently, so their categories may overlap.
    """
    config.validate()
    rng = random.Random(config.seed)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    transactions: list[Transaction] = []
    for index in range(config.rows - config.drop_rows):
        transactions.append({
            "transaction_id": f"{config.batch_id}-tx-{index + 1:06d}",
            "customer_id": f"customer-{rng.randint(1, 100):03d}",
            "product_id": f"product-{rng.randint(1, 10):02d}",
            "quantity": rng.randint(1, 5),
            # Serialize exact decimal cents; binary floating point is unnecessary.
            "unit_price": f"{Decimal(rng.randint(0, 10000)) / 100:.2f}",
            "transaction_timestamp": (start + timedelta(seconds=rng.randrange(86400))).isoformat(),
            "source_file": f"{config.batch_id}.csv",
            "batch_id": config.batch_id,
        })

    distinct_count = len(transactions) - config.duplicate_ids
    for offset in range(config.duplicate_ids):
        # Reuse earlier keys while retaining each row's other generated values.
        transactions[distinct_count + offset]["transaction_id"] = transactions[offset % distinct_count]["transaction_id"]

    for index in rng.sample(range(len(transactions)), config.null_customer_ids):
        transactions[index]["customer_id"] = None
    for index in rng.sample(range(len(transactions)), config.null_product_ids):
        transactions[index]["product_id"] = None
    for index in rng.sample(range(len(transactions)), config.invalid_quantities):
        transactions[index]["quantity"] = 0
    for index in rng.sample(range(len(transactions)), config.invalid_prices):
        transactions[index]["unit_price"] = "-1.00"
    return transactions


def write_batch(config: GenerationConfig, output_dir: Path) -> Path:
    """Create a CSV, reuse identical existing content, or reject a conflict."""
    transactions = generate_transactions(config)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(transactions)
    content = buffer.getvalue().encode("utf-8")

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{config.batch_id}.csv"
    try:
        with path.open("xb") as stream:
            stream.write(content)
    except FileExistsError:
        if path.read_bytes() != content:
            raise FileExistsError(
                f"{path} already contains different data; choose another batch ID or output directory"
            ) from None
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True, help="Business batch identifier and output filename stem")
    parser.add_argument("--rows", type=int, default=100, help="Row count before volume reduction (default: 100)")
    parser.add_argument("--seed", type=int, default=42, help="Repeatable random seed (default: 42)")
    parser.add_argument("--output-dir", type=Path, default=Path("data/source"))
    for flag, description in (
        ("drop-rows", "Number of rows to omit from the requested volume"),
        ("duplicate-ids", "Excess occurrences of transaction IDs; does not add rows"),
        ("null-customer-ids", "Rows with missing customer IDs"),
        ("null-product-ids", "Rows with missing product IDs"),
        ("invalid-quantities", "Rows with quantity set to zero"),
        ("invalid-prices", "Rows with unit_price set to -1.00"),
    ):
        parser.add_argument(f"--{flag}", type=int, default=0, help=description)
    options = vars(parser.parse_args())
    output_dir = options.pop("output_dir")
    try:
        path = write_batch(GenerationConfig(**options), output_dir)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    print(f"Source batch ready: {path}")


if __name__ == "__main__":
    main()
