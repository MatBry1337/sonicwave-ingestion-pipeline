from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from sonicwave_ingest.tables.users import run_users

_INGESTED_AT = datetime(2026, 3, 1)


def _user(
    uid: str,
    email: str | None = "a@sonicwave.io",
    country: str = "PL",
    plan_tier: str = "free",
    created_at: str = "2026-01-10T08:00:00",
    updated_at: str | None = None,
) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        "user_id": uid,
        "email": email,
        "country": country,
        "plan_tier": plan_tier,
        "created_at": created_at,
    }
    if updated_at is not None:
        row["updated_at"] = updated_at
    return row


def _land(source_dir: Path, snapshot: str, rows: list[dict[str, str | None]]) -> None:
    """Write a raw JSON drop under <source>/<snapshot>/."""
    drop = source_dir / snapshot
    drop.mkdir(parents=True)
    (drop / "part-0.json").write_text("\n".join(json.dumps(r) for r in rows))


def _silver(spark: SparkSession, out: str) -> DataFrame:
    return spark.read.parquet(f"{out}/silver/users")


def test_run_users_scd2_end_to_end(spark: SparkSession, tmp_path: Path) -> None:
    source = tmp_path / "source" / "users"
    out = str(tmp_path / "out")
    # T: two clean users + a null-email row that must be quarantined.
    _land(
        source,
        "2026-03-01",
        [
            _user("1", "alice@sonicwave.io", plan_tier="free"),
            _user("2", "bob@sonicwave.io", plan_tier="premium"),
            _user("9", None, plan_tier="free"),
        ],
    )
    # T+1: user 1 upgrades (updated_at set); user 2 unchanged.
    _land(
        source,
        "2026-03-02",
        [
            _user("1", "alice@sonicwave.io", plan_tier="premium", updated_at="2026-03-02T09:00:00"),
            _user("2", "bob@sonicwave.io", plan_tier="premium"),
        ],
    )

    run_users(spark, str(source), "2026-03-01", out, _INGESTED_AT)
    run_users(spark, str(source), "2026-03-02", out, _INGESTED_AT)

    dim = _silver(spark, out)
    # user 1: old free version closed, new premium version open.
    u1 = dim.filter("user_id = 1").orderBy("valid_from").collect()
    assert [r["plan_tier"] for r in u1] == ["free", "premium"]
    assert u1[0]["is_current"] is False
    assert u1[0]["valid_to"] == u1[1]["valid_from"]  # contiguous
    assert u1[1]["is_current"] is True
    assert u1[1]["valid_to"] is None

    # user 2 never changed -> a single open version.
    assert dim.filter("user_id = 2").count() == 1
    # exactly one current row per user.
    assert dim.filter("is_current").count() == dim.select("user_id").distinct().count()

    # the null-email row was quarantined, not historised.
    rejects = spark.read.parquet(f"{out}/quarantine/users")
    assert rejects.filter("reject_reason = 'invalid required field: email'").count() == 1


def test_run_users_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    source = tmp_path / "source" / "users"
    out = str(tmp_path / "out")
    _land(
        source,
        "2026-03-01",
        [_user("1", "alice@sonicwave.io"), _user("2", "bob@sonicwave.io", plan_tier="premium")],
    )

    run_users(spark, str(source), "2026-03-01", out, _INGESTED_AT)
    before = _silver(spark, out).count()

    run_users(spark, str(source), "2026-03-01", out, _INGESTED_AT)  # re-run the same snapshot
    after = _silver(spark, out)

    assert after.count() == before  # no new version opened
    assert after.filter("is_current").count() == after.select("user_id").distinct().count()


def test_run_users_stage_split_matches_stage_all(spark: SparkSession, tmp_path: Path) -> None:
    """Landing then conforming separately must reproduce the single-pass result."""
    source = tmp_path / "source" / "users"
    out_all = str(tmp_path / "out_all")
    out_split = str(tmp_path / "out_split")
    _land(
        source,
        "2026-03-01",
        [_user("1", "alice@sonicwave.io"), _user("2", "bob@sonicwave.io", plan_tier="premium")],
    )

    run_users(spark, str(source), "2026-03-01", out_all, _INGESTED_AT)

    run_users(spark, str(source), "2026-03-01", out_split, _INGESTED_AT, stage="bronze")
    assert not (Path(out_split) / "silver" / "users").exists()

    run_users(spark, str(source), "2026-03-01", out_split, _INGESTED_AT, stage="silver")

    assert _silver(spark, out_split).collect() == _silver(spark, out_all).collect()


def test_run_users_fails_loudly_on_unreadable_dimension(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A dimension path that exists but has no readable parquet (a crash artifact,
    e.g. a job killed after mode("overwrite") deleted the old files but before
    any new ones landed) must raise, not be silently treated as "first run".
    """
    source = tmp_path / "source" / "users"
    out = str(tmp_path / "out")
    _land(source, "2026-03-01", [_user("1", "alice@sonicwave.io")])
    run_users(spark, str(source), "2026-03-01", out, _INGESTED_AT)

    silver_dir = Path(out) / "silver" / "users"
    for parquet_file in silver_dir.glob("*.parquet"):
        parquet_file.unlink()

    _land(source, "2026-03-02", [_user("1", "alice@sonicwave.io", plan_tier="premium")])
    with pytest.raises(AnalysisException):
        run_users(spark, str(source), "2026-03-02", out, _INGESTED_AT)


def test_run_users_preserves_dimension_when_conform_crashes(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash during the dimension write must leave the previous dimension
    untouched (the atomic-swap guarantee), and a clean retry must recover from
    it rather than losing history.
    """
    source = tmp_path / "source" / "users"
    out = str(tmp_path / "out")
    _land(source, "2026-03-01", [_user("1", "alice@sonicwave.io")])
    _land(
        source,
        "2026-03-02",
        [_user("1", "alice@sonicwave.io", plan_tier="premium", updated_at="2026-03-02T09:00:00")],
    )

    run_users(spark, str(source), "2026-03-01", out, _INGESTED_AT)
    before = _silver(spark, out).collect()

    def _simulated_crash(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated crash mid-write")

    monkeypatch.setattr("sonicwave_ingest.tables.users._atomic_swap", _simulated_crash)
    with pytest.raises(RuntimeError):
        run_users(spark, str(source), "2026-03-02", out, _INGESTED_AT)

    # the crashed attempt never touched the previously-published dimension.
    assert _silver(spark, out).collect() == before

    # a clean retry recomputes from the still-intact dimension, not from scratch.
    monkeypatch.undo()
    run_users(spark, str(source), "2026-03-02", out, _INGESTED_AT)
    after = _silver(spark, out)
    assert after.filter("user_id = 1").count() == 2
