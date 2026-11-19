"""A small, local-only Spark session configured to read and write Delta."""

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_local_spark(
    app_name: str = "pipelineiq", *, work_dir: Path = Path(".cache")
) -> SparkSession:
    """Start Spark on this machine; the caller must stop the returned session.

    First startup downloads Delta's JVM dependencies into work_dir/ivy.
    Run this from the project root, or supply an explicit work_dir.
    """
    work_dir = work_dir.resolve()
    for name in ("ivy", "spark", "warehouse"):
        (work_dir / name).mkdir(parents=True, exist_ok=True)

    # Python workers must use the same virtual environment as the driver.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(work_dir / "spark"))
        .config("spark.sql.warehouse.dir", (work_dir / "warehouse").as_uri())
        .config("spark.jars.ivy", str(work_dir / "ivy"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
