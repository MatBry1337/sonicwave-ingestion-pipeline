"""Thin entry-point for the users table.

Parses --source and --snapshot-date, then calls the package to run
source -> Bronze -> Silver (with SCD2) for that one day. No business logic here.
"""

from __future__ import annotations

import argparse
from datetime import datetime

from sonicwave_ingest.entrypoints import resolve_ingested_at
from sonicwave_ingest.spark import build_session
from sonicwave_ingest.tables.users import run_users


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest one users snapshot (source -> Bronze -> Silver SCD2)."
    )
    p.add_argument(
        "--source",
        default=None,
        help="Root of daily drops; <source>/<snapshot-date> is read. "
        "Required unless --stage silver (which re-reads an already-landed Bronze partition).",
    )
    p.add_argument("--snapshot-date", required=True, help="Drop date, YYYY-MM-DD.")
    p.add_argument("--out", required=True, help="Output root for bronze/silver/quarantine.")
    p.add_argument(
        "--stage",
        choices=["all", "bronze", "silver"],
        default="all",
        help="all (default): land + conform in one pass. bronze: land only. "
        "silver: conform an already-landed Bronze partition, without touching source.",
    )
    p.add_argument(
        "--ingested-at",
        default=None,
        help="ISO ingestion timestamp stamped into Bronze. Defaults to snapshot "
        "midnight so a re-run is byte-identical; pass a wall-clock value for real provenance.",
    )
    args = p.parse_args()
    if args.stage != "silver" and args.source is None:
        p.error("--source is required unless --stage silver")
    return args


def main() -> None:
    args = _parse_args()
    snapshot = datetime.strptime(args.snapshot_date, "%Y-%m-%d").date()
    ingested_at = resolve_ingested_at(snapshot, args.ingested_at)
    spark = build_session()
    run_users(spark, args.source, args.snapshot_date, args.out, ingested_at, stage=args.stage)


if __name__ == "__main__":
    main()
