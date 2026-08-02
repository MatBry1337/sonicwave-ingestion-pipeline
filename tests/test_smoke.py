"""Scaffold smoke tests — prove the environment and package are wired up.

These are placeholders that keep CI green from commit one. They get replaced by
real transform tests (quarantine, dedup, SCD2, late-event dating, typed schema)
as each phase lands.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from sonicwave_ingest import __version__


def test_package_imports() -> None:
    assert __version__ == "0.1.0"


def test_spark_runs_a_job(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,), (3,)], "n int")
    assert df.count() == 3
