from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sonicwave_ingest.bronze import read_source
from sonicwave_ingest.storage import write_partitioned

SILVER_PLAYS_SCHEMA = StructType(
    [
        StructField("play_id", LongType(), False),
        StructField("user_id", LongType(), False),
        StructField("content_id", IntegerType(), False),
        StructField("device_id", IntegerType(), False),
        StructField("played_at", TimestampType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), True),
        StructField("ms_played", IntegerType(), False),
    ]
)

SOURCE_PLAYS_SCHEMA = StructType(
    [StructField(f.name, StringType(), True) for f in SILVER_PLAYS_SCHEMA.fields]
)


def _typed_columns() -> list[Column]:
    """Project the raw string columns into the Silver types.

    Built from SILVER_PLAYS_SCHEMA so it can't drift from it. try_cast, not cast,
    returns null on a bad value instead of raising under ANSI mode.
    """
    return [F.col(f.name).try_cast(f.dataType).alias(f.name) for f in SILVER_PLAYS_SCHEMA.fields]


def to_typed(df: DataFrame) -> DataFrame:
    """Type an already-clean plays drop against the Silver schema."""
    return df.select(*_typed_columns())


def _reject_reason() -> Column:
    """First failing rule wins; a clean row yields null.

    Required-field checks are derived from the schema's non-nullable fields, so
    every NOT NULL column is guarded. ms_played is separate: it also carries a
    range rule (>= 0) and its own message.
    """
    required = [
        F.when(
            F.col(f.name).try_cast(f.dataType).isNull(),
            F.lit(f"invalid required field: {f.name}"),
        )
        for f in SILVER_PLAYS_SCHEMA.fields
        if not f.nullable and f.name != "ms_played"
    ]
    return F.coalesce(
        *required,
        F.when(F.col("ms_played").try_cast(IntegerType()).isNull(), F.lit("unparseable ms_played")),
        F.when(F.col("ms_played").try_cast(IntegerType()) < 0, F.lit("ms_played out of range")),
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a raw drop into (clean typed rows, rejects with raw values + reason).

    Clean rows come out typed against SILVER_PLAYS_SCHEMA. Rejects keep their
    original strings plus a `reject_reason` so quarantine is inspectable.
    """
    tagged = df.withColumn("reject_reason", _reject_reason())
    rejects = tagged.filter(F.col("reject_reason").isNotNull())
    clean = tagged.filter(F.col("reject_reason").isNull()).select(*_typed_columns())
    return clean, rejects


def dedup(df: DataFrame) -> DataFrame:
    """Keep one row per play_id via a deterministic row_number() winner.

    The ordering is a total order, so a re-run always keeps the same row (see the
    idempotency note in the README). updated_at is left out: it is null for every
    play, so it can't break a tie.
    """
    order_by = [
        F.col("created_at").desc(),
        F.col("ms_played").desc(),
        F.col("played_at").asc(),
        F.col("content_id").asc(),
        F.col("device_id").asc(),
        F.col("user_id").asc(),
    ]
    window = Window.partitionBy("play_id").orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def with_event_date(df: DataFrame) -> DataFrame:
    """Add event_date = date(played_at), the event time.

    A late play is dated when it happened, not when it arrived; it still lands in
    its arrival snapshot's partition. event_date and snapshot_date stay separate.
    """
    return df.withColumn("event_date", F.to_date("played_at"))


def land_plays(
    spark: SparkSession,
    source_dir: str,
    snapshot_date: str,
    out_root: str,
    ingested_at: datetime,
) -> DataFrame:
    """Read one raw plays drop, write it as Bronze, and return it in memory."""
    bronze = read_source(
        spark, f"{source_dir}/{snapshot_date}", SOURCE_PLAYS_SCHEMA, snapshot_date, ingested_at
    )
    write_partitioned(bronze, f"{out_root}/bronze/plays")
    return bronze


def read_bronze_plays(spark: SparkSession, out_root: str, snapshot_date: str) -> DataFrame:
    """Re-read an already-landed Bronze partition, so Silver can be conformed without re-landing."""
    bronze = spark.read.parquet(f"{out_root}/bronze/plays")
    return bronze.where(F.col("snapshot_date") == snapshot_date)


def conform_plays(bronze: DataFrame, snapshot_date: str, out_root: str) -> None:
    """Validate, dedup, and write Silver + quarantine from a Bronze frame.

    snapshot_date is re-stamped on Silver because the clean projection keeps
    only the typed business columns.
    """
    clean, rejects = split_valid(bronze)
    silver = with_event_date(dedup(clean)).withColumn(
        "snapshot_date", F.lit(snapshot_date).cast(DateType())
    )
    write_partitioned(silver, f"{out_root}/silver/plays")
    write_partitioned(rejects, f"{out_root}/quarantine/plays")


def run_plays(
    spark: SparkSession,
    source_dir: str,
    snapshot_date: str,
    out_root: str,
    ingested_at: datetime,
    *,
    stage: str = "all",
) -> None:
    """Run source -> Bronze -> Silver for one plays snapshot, or just one stage.

    stage="all" (default) lands and conforms from the same in-memory Bronze
    frame, so conform always sees exactly the snapshot that was just landed —
    the single-consistent-as-of guarantee. stage="bronze" only lands.
    stage="silver" re-reads the already-landed Bronze partition and conforms
    it, without touching the source — the seam a backfill-after-landing or a
    granular-retry orchestrator needs.
    """
    if stage == "silver":
        bronze = read_bronze_plays(spark, out_root, snapshot_date)
    else:
        bronze = land_plays(spark, source_dir, snapshot_date, out_root, ingested_at)

    if stage != "bronze":
        conform_plays(bronze, snapshot_date, out_root)
