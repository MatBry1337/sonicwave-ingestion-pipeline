from __future__ import annotations

from datetime import UTC, date, datetime

from sonicwave_ingest.entrypoints import resolve_ingested_at


def test_default_ingested_at_is_utc_midnight() -> None:
    stamp = resolve_ingested_at(date(2026, 3, 1), None)
    # tz-aware UTC, so Spark anchors it independently of the driver's time zone.
    assert stamp.tzinfo is not None
    assert stamp.utcoffset() == UTC.utcoffset(None)
    assert stamp == datetime(2026, 3, 1, tzinfo=UTC)


def test_naive_explicit_stamp_is_read_as_utc() -> None:
    stamp = resolve_ingested_at(date(2026, 3, 1), "2026-03-02T06:00:00")
    assert stamp == datetime(2026, 3, 2, 6, 0, tzinfo=UTC)


def test_offset_explicit_stamp_is_converted_to_utc() -> None:
    # +02:00 wall clock 06:00 is 04:00 UTC.
    stamp = resolve_ingested_at(date(2026, 3, 1), "2026-03-02T06:00:00+02:00")
    assert stamp == datetime(2026, 3, 2, 4, 0, tzinfo=UTC)
