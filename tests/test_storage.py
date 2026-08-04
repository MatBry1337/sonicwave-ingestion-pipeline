from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sonicwave_ingest.storage import write_partitioned


def _snapshot(spark: SparkSession, snapshot_date: str, play_ids: list[int]) -> DataFrame:
    rows = [(pid, snapshot_date) for pid in play_ids]
    return spark.createDataFrame(rows, "play_id long, snapshot_date string")


def _ids(spark: SparkSession, path: str) -> list[int]:
    return sorted(r["play_id"] for r in spark.read.parquet(path).collect())


def test_write_partitioned_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "silver_plays")
    df = _snapshot(spark, "2026-03-01", [1, 2, 3])

    write_partitioned(df, path)
    write_partitioned(df, path)  # re-run the same snapshot

    # Overwrite (not append): still exactly 3 rows, not 6.
    assert _ids(spark, path) == [1, 2, 3]


def test_write_partitioned_overwrites_only_its_own_snapshot(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = str(tmp_path / "silver_plays")

    write_partitioned(_snapshot(spark, "2026-03-01", [1, 2]), path)
    write_partitioned(_snapshot(spark, "2026-03-02", [3]), path)  # a different snapshot
    # T+1 added without wiping T.
    assert _ids(spark, path) == [1, 2, 3]

    # Re-run T with fewer rows: only T's partition changes, T+1 survives.
    write_partitioned(_snapshot(spark, "2026-03-01", [1]), path)
    assert _ids(spark, path) == [1, 3]
