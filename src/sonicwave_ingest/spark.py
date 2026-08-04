"""Shared SparkSession builder, so every job and CI get the same config.

Three settings matter to this pipeline:

* ``partitionOverwriteMode = dynamic`` lets a Silver write overwrite only the
  snapshot partition it loads, which gives ``plays`` per-snapshot idempotency
  without a MERGE.
* ``session.timeZone = UTC`` fixes how offset-less source timestamps are parsed
  and dated, so ``event_date`` and SCD2 timestamps don't depend on the runner.
* ``driver.bindAddress = 127.0.0.1`` keeps Spark on loopback, since a CI runner
  or sandboxed laptop can't always bind to its hostname IP.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_session(app_name: str = "sonicwave-ingest", *, master: str = "local[*]") -> SparkSession:
    """Return a configured local SparkSession for the pipeline."""
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
