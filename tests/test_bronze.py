from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from sonicwave_ingest.bronze import read_source
from sonicwave_ingest.tables.plays import SOURCE_PLAYS_SCHEMA

# A raw line with no updated_at key, as the seed writes when the column is wholly
# null. The explicit schema must still surface updated_at (as null).
_RAW_LINE = (
    '{"play_id":"1000","user_id":"1","content_id":"1","device_id":"1",'
    '"played_at":"2026-03-01T00:00:00","created_at":"2026-03-01T00:00:00","ms_played":"120000"}'
)


def test_read_source_keeps_schema_and_stamps_provenance(
    spark: SparkSession, tmp_path: Path
) -> None:
    drop = tmp_path / "plays" / "2026-03-01"
    drop.mkdir(parents=True)
    (drop / "part-0.json").write_text(_RAW_LINE + "\n")

    ingested_at = datetime(2026, 3, 2, 6, 0, 0)
    df = read_source(spark, str(drop), SOURCE_PLAYS_SCHEMA, "2026-03-01", ingested_at)
    row = df.first()
    assert row is not None
    assert "updated_at" in df.columns
    assert row["updated_at"] is None
    assert row["ms_played"] == "120000"
    assert row["snapshot_date"] == date(2026, 3, 1)
    assert row["source_file"] is not None
    # Provenance is the passed-in stamp, not wall-clock — so a re-run is identical.
    assert row["ingested_at"] == ingested_at
