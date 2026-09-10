"""Shared helpers for the CLI entry-points."""

from __future__ import annotations

from datetime import UTC, date, datetime, time


def resolve_ingested_at(snapshot: date, raw: str | None) -> datetime:
    """Resolve the --ingested-at CLI value into a tz-aware UTC timestamp.

    With no override, ingested_at defaults to the snapshot's UTC midnight — a
    deterministic label for re-run byte-identity, not a real ingestion fact
    (see the README note on provenance). An explicit value is normalized to
    UTC: a naive ISO string is read as UTC (not the process's local time), an
    offset-aware one is converted.

    Built tz-aware here, in Python, because session.timeZone governs how
    Spark parses strings, not how it interprets an already-built Python
    datetime — a naive one handed to F.lit() falls back to the process's
    local time zone regardless of that setting.
    """
    if raw is None:
        return datetime.combine(snapshot, time.min, tzinfo=UTC)
    parsed = datetime.fromisoformat(raw)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
