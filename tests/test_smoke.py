"""Smoke tests: the package imports and Spark can run a job in this environment."""

from __future__ import annotations

from pyspark.sql import SparkSession

from sonicwave_ingest import __version__


def test_package_imports() -> None:
    assert __version__ == "0.1.0"


def test_spark_runs_a_job(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,), (3,)], "n int")
    assert df.count() == 3
