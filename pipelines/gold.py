"""Daily product aggregates from validated Silver transactions."""
from pyspark.sql import DataFrame, functions as F


def aggregate_daily(silver: DataFrame) -> DataFrame:
    """Use the session's UTC dates and exact decimal revenue arithmetic."""
    return silver.withColumn("transaction_date", F.to_date("transaction_timestamp")).groupBy(
        "transaction_date", "product_id"
    ).agg(
        F.count("*").alias("transaction_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
    )
