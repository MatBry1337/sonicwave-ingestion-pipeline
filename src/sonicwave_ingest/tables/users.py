"""The users dimension: schema, quarantine rules, and SCD2 historisation.

SCD2 versions ordered by coalesce(updated_at, created_at); a new version opens
only when a tracked attribute actually changes (the idempotency guard).
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import WindowSpec

from sonicwave_ingest.bronze import read_source
from sonicwave_ingest.storage import write_partitioned

SILVER_USERS_SCHEMA = StructType(
    [
        StructField("user_id", LongType(), False),
        StructField("email", StringType(), False),
        StructField("country", StringType(), False),
        StructField("plan_tier", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), True),
    ]
)

SOURCE_USERS_SCHEMA = StructType(
    [StructField(f.name, StringType(), True) for f in SILVER_USERS_SCHEMA.fields]
)

TRACKED_ATTRIBUTES = ["email", "country", "plan_tier"]


_VERSION_COLUMNS = ["user_id", *TRACKED_ATTRIBUTES, "created_at"]


def _typed_columns() -> list[Column]:
    """Project the raw string columns into the Silver types (schema-driven, ANSI-safe)."""
    return [F.col(f.name).try_cast(f.dataType).alias(f.name) for f in SILVER_USERS_SCHEMA.fields]


def to_typed(df: DataFrame) -> DataFrame:
    """Type an already-clean users drop against the Silver schema."""
    return df.select(*_typed_columns())


def _reject_reason() -> Column:
    """First failing required-field rule wins; a clean row yields null.

    Derived from the schema's non-nullable fields, so a missing email (the seed's
    quarantine row) is caught without hand-listing columns.
    """
    required = [
        F.when(
            F.col(f.name).try_cast(f.dataType).isNull(),
            F.lit(f"invalid required field: {f.name}"),
        )
        for f in SILVER_USERS_SCHEMA.fields
        if not f.nullable
    ]
    return F.coalesce(*required)


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a raw users drop into (clean typed rows, rejects with raw + reason)."""
    tagged = df.withColumn("reject_reason", _reject_reason())
    rejects = tagged.filter(F.col("reject_reason").isNotNull())
    clean = tagged.filter(F.col("reject_reason").isNull()).select(*_typed_columns())
    return clean, rejects


def _effective_ts() -> Column:
    """When a row's current attribute-state became effective."""
    return F.coalesce(F.col("updated_at"), F.col("created_at"))


def dedup(df: DataFrame) -> DataFrame:
    """Keep one row per user_id in a snapshot (drops the seed's duplicate row).

    Deterministic total order so a re-run keeps the same winner; freshest
    effective timestamp first, then the tracked attrs as tiebreaks.
    """
    order_by = [
        _effective_ts().desc(),
        F.col("email").asc(),
        F.col("country").asc(),
        F.col("plan_tier").asc(),
        F.col("created_at").asc(),
    ]
    window = Window.partitionBy("user_id").orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def _version_candidates(incoming: DataFrame, existing: DataFrame | None) -> DataFrame:
    """The rows the SCD2 recompute runs over.

    Each candidate carries its business payload (_VERSION_COLUMNS) and a
    `valid_from` = when that attribute-state became effective. On a non-first run
    the current dimension is unioned in, so the recompute sees history + the new
    snapshot together.
    """
    candidate = incoming.select(*_VERSION_COLUMNS, _effective_ts().alias("valid_from"))
    if existing is None:
        return candidate
    prior = existing.select(*_VERSION_COLUMNS, "valid_from")
    return prior.unionByName(candidate)


def _version_window() -> WindowSpec:
    """Per user, in effective-time order — the frame both SCD2 passes run over."""
    return Window.partitionBy("user_id").orderBy("valid_from", "created_at")


def _change_gate(candidates: DataFrame) -> DataFrame:
    """Drop a candidate whose tracked attributes equal the previous one's.

    Comparing an F.struct of the tracked columns against its F.lag collapses an
    unchanged re-feed onto the version already on record (nothing changed, so no
    new version opens) — the idempotency guard. A first row (lag is null) and a
    real change both survive.
    """
    tracked = F.struct(*TRACKED_ATTRIBUTES)
    previous = F.lag(tracked).over(_version_window())
    changed = previous.isNull() | (tracked != previous)

    return candidates.withColumn("_changed", changed).filter(F.col("_changed")).drop("_changed")


def _with_validity(versions: DataFrame) -> DataFrame:
    """Close each version at the next one's valid_from; the latest stays open."""
    valid_to = F.lead("valid_from").over(_version_window())
    return versions.withColumn("valid_to", valid_to).withColumn(
        "is_current", F.col("valid_to").isNull()
    )


def scd2_merge(incoming: DataFrame, existing: DataFrame | None) -> DataFrame:
    """Recompute SCD2 validity over incoming + the current dimension.

    `incoming` is a typed, deduped snapshot (one row per user_id). `existing` is
    the current dimension, or None on the first run.
    Output columns: _VERSION_COLUMNS + valid_from, valid_to, is_current.
    """
    candidates = _version_candidates(incoming, existing)
    return _with_validity(_change_gate(candidates))


def _dimension_exists(spark: SparkSession, path: str) -> bool:
    """Whether `path` already exists, via Spark's own Hadoop FileSystem.

    Works for whatever backend the session is configured against (local disk,
    HDFS, S3, DBFS...) with the exact same path resolution spark.read.parquet
    uses — unlike a plain os.path check, which silently reports False for any
    non-local URI.
    """
    assert spark._jvm is not None  # always set on a live SparkSession
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    return bool(jvm_path.getFileSystem(hadoop_conf).exists(jvm_path))


def _atomic_swap(spark: SparkSession, tmp_path: str, final_path: str) -> None:
    """Publish tmp_path as final_path without ever leaving final_path partially written.

    final_path is only touched once tmp_path is fully written and this rename
    runs, so a job killed mid-write leaves the previous dimension at final_path
    completely untouched — the crashed run just leaves a disposable tmp_path
    behind, cleaned up by the next attempt's overwrite.
    """
    assert spark._jvm is not None  # always set on a live SparkSession
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    tmp = spark._jvm.org.apache.hadoop.fs.Path(tmp_path)
    final = spark._jvm.org.apache.hadoop.fs.Path(final_path)
    if fs.exists(final):
        fs.delete(final, True)
    fs.rename(tmp, final)


def _read_existing(spark: SparkSession, path: str) -> DataFrame | None:
    """Load the current dimension, or None if this is the first run.

    Checks existence explicitly instead of catching AnalysisException, so a
    path that exists but can't be read (e.g. left partial by a job killed
    mid-write) fails loudly instead of being silently treated as "no
    dimension yet" — see the point 1 write-up for why that matters.

    localCheckpoint truncates the frame's lineage so it no longer depends on
    `path` — needed because conform_users writes a new dimension while this
    one is still being read. Safe here specifically because that write goes
    to a temp path and is swapped in atomically (see _atomic_swap): `path` is
    never the write target, so this checkpoint is never the only copy of
    history in flight.
    """
    if not _dimension_exists(spark, path):
        return None
    return spark.read.parquet(path).localCheckpoint(eager=True)


def land_users(
    spark: SparkSession,
    source_dir: str,
    snapshot_date: str,
    out_root: str,
    ingested_at: datetime,
) -> DataFrame:
    """Read one raw users drop, write it as Bronze, and return it in memory."""
    bronze = read_source(
        spark, f"{source_dir}/{snapshot_date}", SOURCE_USERS_SCHEMA, snapshot_date, ingested_at
    )
    write_partitioned(bronze, f"{out_root}/bronze/users")
    return bronze


def read_bronze_users(spark: SparkSession, out_root: str, snapshot_date: str) -> DataFrame:
    """Re-read an already-landed Bronze partition, to conform without re-landing."""
    bronze = spark.read.parquet(f"{out_root}/bronze/users")
    return bronze.where(F.col("snapshot_date") == snapshot_date)


def conform_users(spark: SparkSession, bronze: DataFrame, out_root: str) -> None:
    """Validate, dedup, and recompute the SCD2 dimension from a Bronze frame."""
    clean, rejects = split_valid(bronze)
    snapshot = dedup(clean)

    silver_path = f"{out_root}/silver/users"
    tmp_path = f"{silver_path}_tmp"
    dimension = scd2_merge(snapshot, _read_existing(spark, silver_path))
    dimension.write.mode("overwrite").parquet(tmp_path)
    _atomic_swap(spark, tmp_path, silver_path)

    write_partitioned(rejects, f"{out_root}/quarantine/users")


def run_users(
    spark: SparkSession,
    source_dir: str,
    snapshot_date: str,
    out_root: str,
    ingested_at: datetime,
    *,
    stage: str = "all",
) -> None:
    """Run source -> Bronze -> Silver (SCD2) for one users snapshot, or just one stage.

    Mirrors run_plays's land/conform split. Users is an SCD2 dimension, so
    conform recomputes the whole history and overwrites it as one table, not
    partitioned by snapshot_date the way plays is. A version spans a range of
    snapshots (valid_from..valid_to), so it belongs to no single one.
    """
    if stage == "silver":
        bronze = read_bronze_users(spark, out_root, snapshot_date)
    else:
        bronze = land_users(spark, source_dir, snapshot_date, out_root, ingested_at)

    if stage != "bronze":
        conform_users(spark, bronze, out_root)
