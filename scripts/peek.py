"""Dev helper: print a quick summary of a warehouse layer.

    uv run python scripts/peek.py silver        # ./data/warehouse/silver/plays
    uv run python scripts/peek.py bronze
    uv run python scripts/peek.py quarantine
    uv run python scripts/peek.py ./data/warehouse/silver/plays   # or a raw path

Reads the parquet, prints row count + schema + a sample. Not part of the wheel;
it just exists so you can eyeball what a run produced.
"""

from __future__ import annotations

import sys

from sonicwave_ingest.spark import build_session

DEFAULTS = {
    "bronze": "./data/warehouse/bronze/plays",
    "silver": "./data/warehouse/silver/plays",
    "quarantine": "./data/warehouse/quarantine/plays",
}


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else "silver"
    path = DEFAULTS.get(arg, arg)

    spark = build_session("peek")
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(path)
    print(f"\n{path}: {df.count()} rows")
    df.printSchema()
    df.show(10, truncate=False)

    if "snapshot_date" in df.columns:
        print("rows per snapshot:")
        df.groupBy("snapshot_date").count().orderBy("snapshot_date").show()

    spark.stop()


if __name__ == "__main__":
    main()
