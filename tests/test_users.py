"""Tests for the users table: typed schema, quarantine, dedup, and SCD2.

The SCD2 tests drive `scd2_merge` on hand-built 2-user snapshots: a first run,
a tracked-attribute change (old version closes, new one opens), and a re-fed
identical snapshot that must open no new version (the idempotency guard).
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from sonicwave_ingest.tables.users import (
    SILVER_USERS_SCHEMA,
    dedup,
    scd2_merge,
    split_valid,
    to_typed,
)

SOURCE_COLS = ["user_id", "email", "country", "plan_tier", "created_at", "updated_at"]
_SOURCE_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLS])


def _row(
    user_id: str = "1",
    email: str | None = "alice@sonicwave.io",
    country: str = "PL",
    plan_tier: str = "free",
    created_at: str = "2026-01-10T08:00:00",
    updated_at: str | None = None,
) -> tuple[Any, ...]:
    """One raw (all-string) users row; override only the field a test cares about."""
    return (user_id, email, country, plan_tier, created_at, updated_at)


def _source(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, schema=_SOURCE_SCHEMA)


def _snapshot(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    """A typed, deduped users snapshot — exactly what scd2_merge consumes."""
    clean, _rejects = split_valid(_source(spark, rows))
    return dedup(clean)


def _dim_with_one_change(spark: SparkSession) -> DataFrame:
    """Two snapshots: user 1 upgrades free -> premium at T+1; user 2 never changes."""
    snap_t = _snapshot(
        spark,
        [
            _row(user_id="1", plan_tier="free"),
            _row(user_id="2", email="bob@sonicwave.io", plan_tier="premium"),
        ],
    )
    snap_t1 = _snapshot(
        spark,
        [
            _row(user_id="1", plan_tier="premium", updated_at="2026-03-02T09:00:00"),
            _row(user_id="2", email="bob@sonicwave.io", plan_tier="premium"),
        ],
    )
    return scd2_merge(snap_t1, scd2_merge(snap_t, None))


def test_to_typed_matches_declared_schema(spark: SparkSession) -> None:
    """to_typed projects a clean drop to exactly SILVER_USERS_SCHEMA's names+types."""
    df = _source(spark, [_row(user_id="1"), _row(user_id="2", email="bob@sonicwave.io")])
    typed = to_typed(df)
    got = [(f.name, f.dataType) for f in typed.schema.fields]
    want = [(f.name, f.dataType) for f in SILVER_USERS_SCHEMA.fields]
    assert got == want


def test_split_valid_quarantines_null_email(spark: SparkSession) -> None:
    """A null-email row (the seed's messy row) is rejected; a clean row survives."""
    df = _source(spark, [_row(user_id="1"), _row(user_id="11", email=None)])
    clean, rejects = split_valid(df)

    assert clean.count() == 1
    assert clean.first()["user_id"] == 1

    assert rejects.count() == 1
    assert rejects.first()["reject_reason"] == "invalid required field: email"


def test_split_valid_keeps_clean_rows(spark: SparkSession) -> None:
    """A drop of only-clean rows produces zero rejects, all rows typed."""
    df = _source(spark, [_row(user_id="1"), _row(user_id="2", email="bob@sonicwave.io")])
    clean, rejects = split_valid(df)

    assert rejects.count() == 0
    assert clean.count() == 2

    got = [(f.name, f.dataType) for f in clean.schema.fields]
    want = [(f.name, f.dataType) for f in SILVER_USERS_SCHEMA.fields]
    assert got == want


def test_dedup_collapses_duplicate_user_row(spark: SparkSession) -> None:
    """The seed ships user 4 twice in the same drop -> dedup keeps exactly one."""
    df = _source(spark, [_row(user_id="1"), _row(user_id="1")])
    clean, _rejects = split_valid(df)

    assert clean.count() == 2
    assert dedup(clean).count() == 1


def test_dedup_is_deterministic(spark: SparkSession) -> None:
    """Feeding the duplicate pair in either order yields the same surviving row."""
    # Same user_id; the premium row changed later, so its effective_ts is newer.
    free = _row(user_id="1", plan_tier="free")
    premium = _row(user_id="1", plan_tier="premium", updated_at="2026-03-02T09:00:00")
    for rows in ([free, premium], [premium, free]):
        clean, _rejects = split_valid(_source(spark, rows))
        out = dedup(clean)
        assert out.count() == 1
        assert out.first()["plan_tier"] == "premium"


def test_scd2_first_run_opens_one_open_version_per_user(spark: SparkSession) -> None:
    """existing=None: every user gets exactly one row, is_current=True, valid_to=null."""
    snap = _snapshot(spark, [_row(user_id="1"), _row(user_id="2", email="bob@sonicwave.io")])
    dim = scd2_merge(snap, None)

    assert dim.count() == 2
    assert dim.filter("is_current").count() == 2
    assert dim.filter("valid_to is not null").count() == 0


def test_scd2_opens_new_version_on_attribute_change(spark: SparkSession) -> None:
    """A user changes plan_tier between snapshots (updated_at flips null -> a time):
    the old version closes (valid_to set, is_current=False) and a new open version
    opens. The unchanged user stays a single open version.
    """
    dim = _dim_with_one_change(spark)

    u1 = dim.filter("user_id = 1").orderBy("valid_from").collect()
    assert len(u1) == 2
    assert u1[0]["plan_tier"] == "free"
    assert u1[0]["is_current"] is False
    assert u1[0]["valid_to"] is not None
    assert u1[1]["plan_tier"] == "premium"
    assert u1[1]["is_current"] is True
    assert u1[1]["valid_to"] is None

    assert dim.filter("user_id = 2").count() == 1


def test_scd2_exactly_one_current_row_per_user(spark: SparkSession) -> None:
    """After a change, each user_id has exactly one row with is_current=True."""
    dim = _dim_with_one_change(spark)

    per_user = dim.filter("is_current").groupBy("user_id").count().collect()
    assert all(r["count"] == 1 for r in per_user)
    assert dim.filter("is_current").count() == dim.select("user_id").distinct().count()


def test_scd2_intervals_are_contiguous_and_non_overlapping(spark: SparkSession) -> None:
    """Per user, a closed version's valid_to equals the next version's valid_from,
    and no two intervals overlap.
    """
    dim = _dim_with_one_change(spark)

    u1 = dim.filter("user_id = 1").orderBy("valid_from").collect()
    assert u1[0]["valid_to"] == u1[1]["valid_from"]
    assert u1[-1]["valid_to"] is None


def test_scd2_no_new_version_on_unchanged_resnapshot(spark: SparkSession) -> None:
    """Re-applying the same snapshot opens no new version (the change-gate).

    scd2_merge(snapshot, existing), where existing already contains that snapshot,
    must return the same version count as before.
    """
    snap = _snapshot(spark, [_row(user_id="1"), _row(user_id="2", email="bob@sonicwave.io")])
    first = scd2_merge(snap, None)
    again = scd2_merge(snap, first)

    assert again.count() == first.count()
    assert again.filter("is_current").count() == again.select("user_id").distinct().count()
