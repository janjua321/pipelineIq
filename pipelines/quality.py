"""Retail quality policies expressed as reusable Spark transformations."""

from pyspark.sql import Column, DataFrame, Window, functions as F

from pipelines.bronze import SOURCE_SCHEMA


ID_COLUMNS = ("transaction_id", "customer_id", "product_id")
VALUE_COLUMNS = ("quantity", "unit_price", "transaction_timestamp")
DERIVED_COLUMNS = tuple(f"normalized_{name}" for name in ID_COLUMNS) + tuple(
    f"parsed_{name}" for name in VALUE_COLUMNS
)
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX"


def _trimmed(name: str) -> Column:
    return F.regexp_replace(F.col(name), r"^\s+|\s+$", "")


def _missing(name: str) -> Column:
    return F.col(name).isNull() | (_trimmed(name) == "")


def classify_transactions(bronze: DataFrame) -> DataFrame:
    """Retain every raw row and add parsed values, a source hash, and reason codes.

    Duplicates are scoped to this input batch and use trimmed transaction IDs.
    Prefer otherwise-valid rows, then earliest parsed time, then original source
    values in schema order. Exact duplicate source rows are interchangeable.
    """
    missing_columns = set(SOURCE_SCHEMA.fieldNames()) - set(bronze.columns)
    if missing_columns:
        raise ValueError(f"Missing source columns: {', '.join(sorted(missing_columns))}")
    result = bronze
    for name in ID_COLUMNS:
        result = result.withColumn(
            f"normalized_{name}", F.when(~_missing(name), _trimmed(name))
        )

    quantity = _trimmed("quantity")
    price = _trimmed("unit_price")
    result = (
        result.withColumn(
            "parsed_quantity",
            F.when(quantity.rlike(r"^[+-]?[0-9]+$"), quantity.try_cast("int")),
        )
        .withColumn(
            "parsed_unit_price",
            # Check scale before casting: DECIMAL casting alone can round silently.
            F.when(price.rlike(r"^[+-]?[0-9]+(?:\.[0-9]{1,2})?$"), price.try_cast("decimal(18,2)")),
        )
        .withColumn(
            "parsed_transaction_timestamp",
            F.try_to_timestamp(_trimmed("transaction_timestamp"), F.lit(TIMESTAMP_FORMAT)),
        )
    )
    checks = [
        (_missing("transaction_id"), "MISSING_TRANSACTION_ID"),
        (_missing("customer_id"), "MISSING_CUSTOMER_ID"),
        (_missing("product_id"), "MISSING_PRODUCT_ID"),
        (_missing("transaction_timestamp"), "MISSING_TRANSACTION_TIMESTAMP"),
        (~_missing("transaction_timestamp") & F.col("parsed_transaction_timestamp").isNull(), "INVALID_TRANSACTION_TIMESTAMP"),
        (F.col("parsed_quantity").isNull() | (F.col("parsed_quantity") <= 0), "INVALID_QUANTITY"),
        (F.col("parsed_unit_price").isNull() | (F.col("parsed_unit_price") < 0), "INVALID_UNIT_PRICE"),
    ]
    result = (
        result.withColumn(
            "_base_reasons",
            F.filter(F.array(*[F.when(condition, F.lit(code)) for condition, code in checks]), lambda value: value.isNotNull()),
        )
        .withColumn(
            "_source_payload",
            F.to_json(F.struct(*SOURCE_SCHEMA.fieldNames()), options={"ignoreNullFields": "false"}),
        )
        .withColumn("source_record_hash", F.sha2("_source_payload", 256))
    )
    order = Window.partitionBy("normalized_transaction_id").orderBy(
        F.when(F.size("_base_reasons") == 0, 0).otherwise(1),
        F.col("parsed_transaction_timestamp").asc_nulls_last(),
        F.col("_source_payload").asc(),
    )
    result = result.withColumn("_duplicate_rank", F.row_number().over(order))
    duplicate = F.col("normalized_transaction_id").isNotNull() & (F.col("_duplicate_rank") > 1)
    return (
        result.withColumn(
            "reason_codes",
            F.filter(
                F.concat("_base_reasons", F.array(F.when(duplicate, F.lit("DUPLICATE_TRANSACTION_ID")))),
                lambda value: value.isNotNull(),
            ),
        )
        .drop("_base_reasons", "_source_payload", "_duplicate_rank")
    )


def split_classified(classified: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return typed Silver rows and original quarantined rows with their reasons."""
    silver = classified.filter(F.size("reason_codes") == 0)
    for name in ID_COLUMNS:
        silver = silver.withColumn(name, F.col(f"normalized_{name}"))
    for name in VALUE_COLUMNS:
        silver = silver.withColumn(name, F.col(f"parsed_{name}"))
    quarantine = classified.filter(F.size("reason_codes") > 0)
    return (
        silver.drop(*DERIVED_COLUMNS, "reason_codes"),
        quarantine.drop(*DERIVED_COLUMNS),
    )
