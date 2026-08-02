"""Shared pytest fixtures.

`conftest.py` is pytest's magic file: anything defined here is available to
every test in this tree without an import. The SparkSession fixture lives
here — one Spark, shared by the whole suite, session-scoped so we pay the JVM
start-up cost once.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("sonicwave-ingest-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()
