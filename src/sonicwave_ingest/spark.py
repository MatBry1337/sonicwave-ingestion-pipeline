"""SparkSession construction shared by the entry-points and the seed.

One place builds the session so every job — and CI — gets the same engine
configuration. Two settings here are load-bearing for this pipeline:

* ``spark.sql.sources.partitionOverwriteMode = dynamic`` — lets a Silver write
  overwrite only the snapshot partition it is loading, which is how ``plays``
  gets per-snapshot idempotency without a ``MERGE``.
* ``spark.driver.bindAddress = 127.0.0.1`` — a hosted CI runner (or a
  sandboxed / VPN'd laptop) can't always bind to its hostname IP; loopback
  always works.
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
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
