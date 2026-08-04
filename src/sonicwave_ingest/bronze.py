from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, TimestampType


def read_source(
    spark: SparkSession,
    path: str,
    schema: StructType,
    snapshot_date: str,
    ingested_at: datetime,
) -> DataFrame:
    """Land a raw daily drop as Bronze: permissive read + provenance.

    The explicit all-string schema keeps a wholly-null column (e.g. plays.updated_at,
    which the JSON writer omits) present instead of vanishing under inference. No
    typing or cleaning here — that is Silver's job. ingested_at is passed in, not
    current_timestamp(), so a re-run yields identical Bronze provenance.
    """
    return (
        spark.read.schema(schema)
        .json(path)
        .withColumn("ingested_at", F.lit(ingested_at).cast(TimestampType()))
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast(DateType()))
    )
