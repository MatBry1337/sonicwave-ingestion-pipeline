from __future__ import annotations

from pyspark.sql import DataFrame


def write_partitioned(df: DataFrame, path: str, *, partition_by: str = "snapshot_date") -> None:
    """Write `df` as Parquet, overwriting only the partitions present in `df`.

    Re-running one snapshot rewrites just its partition and leaves the others
    untouched, which is the per-snapshot idempotency plays relies on. Used for the
    Bronze landing, the Silver write, and the reject sink.

    Requires `partitionOverwriteMode=dynamic` (set in `build_session`); without it
    `mode("overwrite")` would wipe the whole path.
    """
    df.write.mode("overwrite").partitionBy(partition_by).parquet(path)
