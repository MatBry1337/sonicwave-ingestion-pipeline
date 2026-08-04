from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType

from sonicwave_ingest.tables.plays import (
    SILVER_PLAYS_SCHEMA,
    dedup,
    split_valid,
    to_typed,
    with_event_date,
)

SOURCE_COLS = [
    "play_id",
    "user_id",
    "content_id",
    "device_id",
    "played_at",
    "created_at",
    "updated_at",
    "ms_played",
]
_SOURCE_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLS])


def _clean_source(spark: SparkSession) -> DataFrame:
    rows = [
        ("1000", "1", "1", "1", "2026-03-01T00:00:00", "2026-03-01T00:00:00", None, "200000"),
        ("1001", "2", "2", "2", "2026-03-01T00:07:00", "2026-03-01T00:07:00", None, "123456"),
    ]
    return spark.createDataFrame(rows, schema=_SOURCE_SCHEMA)


def _names_and_types(schema: StructType) -> list[tuple[str, object]]:
    return [(f.name, f.dataType) for f in schema.fields]


def test_to_typed_matches_declared_schema(spark: SparkSession) -> None:
    typed = to_typed(_clean_source(spark))
    assert _names_and_types(typed.schema) == _names_and_types(SILVER_PLAYS_SCHEMA)


def test_to_typed_casts_values(spark: SparkSession) -> None:
    typed = to_typed(_clean_source(spark)).orderBy("play_id")
    assert typed.count() == 2
    first = typed.first()
    assert first is not None
    assert first["play_id"] == 1000
    assert first["ms_played"] == 200000


def _row(
    play_id="1000",
    user_id="1",
    content_id="1",
    device_id="1",
    played_at="2026-03-01T00:00:00",
    created_at="2026-03-01T00:00:00",
    updated_at=None,
    ms_played="200000",
):
    """One raw (all-string) plays row; override only the field a test cares about."""
    return (play_id, user_id, content_id, device_id, played_at, created_at, updated_at, ms_played)


def _source(spark: SparkSession, rows: list) -> DataFrame:
    return spark.createDataFrame(rows, schema=_SOURCE_SCHEMA)


def _only_reason(rejects: DataFrame) -> str:
    row = rejects.first()
    assert row is not None
    return row["reject_reason"]


@pytest.mark.parametrize(
    ("bad_row", "reason"),
    [
        (_row(play_id="1001", user_id=None), "invalid required field: user_id"),
        # "NaN" must not crash the job under ANSI; it is quarantined instead.
        (_row(play_id="1001", ms_played="NaN"), "unparseable ms_played"),
        (_row(play_id="1001", ms_played="-5000"), "ms_played out of range"),
    ],
)
def test_split_valid_quarantines_bad_rows(spark: SparkSession, bad_row: tuple, reason: str) -> None:
    df = _source(spark, [_row(play_id="1000"), bad_row])
    clean, rejects = split_valid(df)

    assert clean.count() == 1  # the good row survives
    assert clean.first()["play_id"] == 1000
    assert rejects.count() == 1
    assert _only_reason(rejects) == reason
    assert rejects.first()["play_id"] == "1001"  # rejects keep their raw strings


def test_split_valid_keeps_clean_rows(spark: SparkSession) -> None:
    clean, rejects = split_valid(_clean_source(spark))
    assert clean.count() == 2
    assert rejects.count() == 0
    assert _names_and_types(clean.schema) == _names_and_types(SILVER_PLAYS_SCHEMA)


def test_dedup_keeps_the_deterministic_winner(spark: SparkSession) -> None:
    # Two copies of play_id 1000 with the same created_at (the seed's own dup):
    # created_at ties, so the winner must be decided by the ordering's tiebreak
    # (ms_played desc), never by input order. Feed both orderings and require the
    # same winner each time, which is the idempotency guarantee under test.
    winner = _row(play_id="1000", ms_played="200000")
    loser = _row(play_id="1000", ms_played="120000")
    for rows in ([winner, loser], [loser, winner]):
        out = dedup(split_valid(_source(spark, rows))[0])
        assert out.count() == 1
        assert out.first()["ms_played"] == 200000


def test_dedup_leaves_distinct_ids_untouched(spark: SparkSession) -> None:
    rows = [_row(play_id="1000"), _row(play_id="1001"), _row(play_id="1002")]
    clean, _rejects = split_valid(_source(spark, rows))
    assert dedup(clean).count() == 3


def test_dedup_is_idempotent(spark: SparkSession) -> None:
    rows = [_row(play_id="1000", ms_played="120000"), _row(play_id="1000", ms_played="200000")]
    clean, _rejects = split_valid(_source(spark, rows))
    assert dedup(clean).collect() == dedup(dedup(clean)).collect()


def test_with_event_date_dates_by_event_time_not_arrival(spark: SparkSession) -> None:
    # Late play: happened on 2026-03-01 but recorded (created_at) on 2026-03-02.
    rows = [_row(play_id="2900", played_at="2026-03-01T21:30:00", created_at="2026-03-02T06:00:00")]
    clean, _rejects = split_valid(_source(spark, rows))
    row = with_event_date(clean).first()
    assert row is not None
    assert row["event_date"] == date(2026, 3, 1)  # played_at, not created_at


def test_with_event_date_is_a_date_column(spark: SparkSession) -> None:
    clean, _rejects = split_valid(_clean_source(spark))
    out = with_event_date(clean)
    assert out.schema["event_date"].dataType == DateType()
