from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sonicwave_ingest.tables.plays import run_plays

_SNAPSHOT = "2026-03-01"
_INGESTED_AT = datetime(2026, 3, 1)

# One drop with a clean row, a dedup collision, and two quarantine cases.
_ROWS = [
    {
        "play_id": "1000",
        "user_id": "1",
        "content_id": "1",
        "device_id": "1",
        "played_at": f"{_SNAPSHOT}T00:00:00",
        "created_at": f"{_SNAPSHOT}T00:00:00",
        "ms_played": "120000",
    },
    # duplicate play_id 1000, higher ms_played -> the deterministic winner
    {
        "play_id": "1000",
        "user_id": "1",
        "content_id": "1",
        "device_id": "1",
        "played_at": f"{_SNAPSHOT}T00:00:00",
        "created_at": f"{_SNAPSHOT}T00:00:00",
        "ms_played": "200000",
    },
    {
        "play_id": "1001",
        "user_id": "2",
        "content_id": "2",
        "device_id": "2",
        "played_at": f"{_SNAPSHOT}T01:00:00",
        "created_at": f"{_SNAPSHOT}T01:00:00",
        "ms_played": "150000",
    },
    # null user_id -> quarantine
    {
        "play_id": "1002",
        "user_id": None,
        "content_id": "3",
        "device_id": "1",
        "played_at": f"{_SNAPSHOT}T02:00:00",
        "created_at": f"{_SNAPSHOT}T02:00:00",
        "ms_played": "180000",
    },
    # negative ms_played -> quarantine
    {
        "play_id": "1003",
        "user_id": "3",
        "content_id": "4",
        "device_id": "2",
        "played_at": f"{_SNAPSHOT}T03:00:00",
        "created_at": f"{_SNAPSHOT}T03:00:00",
        "ms_played": "-5000",
    },
]


def _land_drop(tmp_path: Path) -> str:
    """Write the raw JSON drop under <source>/<snapshot>/ and return <source>."""
    source = tmp_path / "source" / "plays"
    drop = source / _SNAPSHOT
    drop.mkdir(parents=True)
    (drop / "part-0.json").write_text("\n".join(json.dumps(r) for r in _ROWS))
    return str(source)


def _silver(spark: SparkSession, out_root: str) -> DataFrame:
    return spark.read.parquet(f"{out_root}/silver/plays")


def test_run_plays_end_to_end(spark: SparkSession, tmp_path: Path) -> None:
    source = _land_drop(tmp_path)
    out = str(tmp_path / "out")

    run_plays(spark, source, _SNAPSHOT, out, _INGESTED_AT)

    silver = _silver(spark, out)
    # 1000 (deduped) + 1001; the two malformed rows are gone.
    assert sorted(r["play_id"] for r in silver.collect()) == [1000, 1001]
    won = silver.where("play_id = 1000").first()
    assert won is not None
    assert won["ms_played"] == 200000  # the deterministic dedup winner
    assert won["event_date"] == date(2026, 3, 1)
    assert won["snapshot_date"] == date(2026, 3, 1)

    quarantine = spark.read.parquet(f"{out}/quarantine/plays")
    reasons = {r["reject_reason"] for r in quarantine.collect()}
    assert reasons == {"invalid required field: user_id", "ms_played out of range"}


def test_run_plays_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    source = _land_drop(tmp_path)
    out = str(tmp_path / "out")

    run_plays(spark, source, _SNAPSHOT, out, _INGESTED_AT)
    first = _silver(spark, out).collect()

    run_plays(spark, source, _SNAPSHOT, out, _INGESTED_AT)
    second = _silver(spark, out).collect()

    assert first == second
