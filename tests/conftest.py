"""Shared pytest fixtures — a session-scoped SparkSession so the JVM starts once."""

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
