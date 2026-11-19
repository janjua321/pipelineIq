"""Exercise Python workers, the JVM, and real Delta persistence together."""

from importlib.metadata import version
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from pipelines.spark import create_local_spark


@pytest.mark.integration
def test_spark_delta_round_trip(tmp_path: Path) -> None:
    spark = create_local_spark("pipelineiq-smoke")
    try:
        # An explicit schema makes the expected column type unambiguous.
        source = spark.createDataFrame([(1,), (2,), (3,)], schema="value long")
        transformed = source.withColumn("doubled", F.col("value") * 2)
        table_path = tmp_path / "smoke_delta"

        # save() is an action: it executes the pending transformation.
        transformed.write.format("delta").mode("errorifexists").save(str(table_path))
        restored = spark.read.format("delta").load(str(table_path))
        actual = [tuple(row) for row in restored.orderBy("value").collect()]

        assert actual == [(1, 2), (2, 4), (3, 6)]
        assert restored.schema.simpleString() == "struct<value:bigint,doubled:bigint>"
        assert (table_path / "_delta_log" / "00000000000000000000.json").is_file()
        print(f"\nSpark {spark.version}; Delta {version('delta-spark')}")
        print(f"Delta round trip: {actual}")
        print(f"Temporary Delta table: {table_path}")
    finally:
        spark.stop()
