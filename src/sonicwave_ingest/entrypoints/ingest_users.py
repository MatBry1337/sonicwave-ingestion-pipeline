"""Thin entry-point for the users table.

Parses --source and --snapshot-date, then calls the package to run
source -> Bronze -> Silver (with SCD2) for that one day. No business logic here.
"""

from __future__ import annotations

import argparse
from datetime import datetime, time

from sonicwave_ingest.spark import build_session
from sonicwave_ingest.tables.users import run_users


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest one users snapshot (source -> Bronze -> Silver SCD2)."
    )
    p.add_argument(
        "--source", required=True, help="Root of daily drops; <source>/<snapshot-date> is read."
    )
    p.add_argument("--snapshot-date", required=True, help="Drop date, YYYY-MM-DD.")
    p.add_argument("--out", required=True, help="Output root for bronze/silver/quarantine.")
    p.add_argument(
        "--ingested-at",
        default=None,
        help="ISO ingestion timestamp stamped into Bronze. Defaults to snapshot "
        "midnight so a re-run is byte-identical; pass a wall-clock value for real provenance.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    snapshot = datetime.strptime(args.snapshot_date, "%Y-%m-%d").date()
    ingested_at = (
        datetime.fromisoformat(args.ingested_at)
        if args.ingested_at
        else datetime.combine(snapshot, time.min)
    )
    spark = build_session()
    run_users(spark, args.source, args.snapshot_date, args.out, ingested_at)


if __name__ == "__main__":
    main()
