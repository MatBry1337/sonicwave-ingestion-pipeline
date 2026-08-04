# SonicWave Ingestion Pipeline

Module 2 capstone. Turns SonicWave's raw **daily source drops** into clean,
typed, conformed **Silver** tables — reliably, repeatably, and safely
re-runnable — using PySpark over files.

The shape to internalise: **the entry-point is thin and the logic is a
versioned package.** One small script per table parses arguments and calls the
package; all real work (read → Bronze → Silver, validation, historisation)
lives in the installable wheel under `src/`.

---

## Status

- [x] Scaffold: `src/` package, `pyproject.toml`, CI + release workflows, pre-commit.
- [x] `plays`: source → Bronze → Silver for one snapshot — schema-on-read,
      quarantine, dedup, `event_date`, partition-overwrite idempotency.
- [x] `users`: SCD2 dimension — change-gated version history, quarantine, dedup.
- [x] Tests: transform unit tests plus end-to-end pipeline tests for both tables,
      including re-run idempotency.
- [x] CI/CD: lint → type → test gate on every PR; build-once release.

---

## Quickstart

```bash
uv sync                                    # install package + dev tooling
uv run python seed/generate_seed.py        # -> ./data/source/<table>/<date>/*.json

# run a table through source -> Bronze -> Silver for one day:
uv run python -m sonicwave_ingest.entrypoints.ingest_plays \
    --source ./data/source/plays --snapshot-date 2026-03-01 --out ./data/warehouse
uv run python -m sonicwave_ingest.entrypoints.ingest_users \
    --source ./data/source/users --snapshot-date 2026-03-01 --out ./data/warehouse

# inspect a layer (bronze | silver | quarantine, or an explicit path):
uv run python scripts/peek.py silver
```

### The quality gate (same commands locally and in CI)

```bash
uv run ruff check .            # lint
uv run ruff format --check .   # formatting
uv run mypy                    # static types
uv run pytest -q               # tests
uv build                       # sdist + wheel into dist/
uv run pre-commit install      # wire ruff + mypy to run on every commit
```

Requires a JDK on PATH (Spark runs on the JVM) — Java 17 recommended.

---

## Layout

```
.
├── pyproject.toml              # package metadata, deps, ruff/mypy/pytest config
├── seed/generate_seed.py       # the SOURCE OF TRUTH for the data — commit it
├── src/sonicwave_ingest/       # all pipeline logic (the wheel)
│   ├── spark.py                # shared SparkSession builder
│   ├── bronze.py               # permissive read + provenance
│   ├── storage.py              # partitioned Parquet write
│   ├── tables/                 # per-table schema, validation, conform (plays, users)
│   └── entrypoints/            # thin CLI scripts, one per table
├── scripts/peek.py             # dev helper to eyeball a warehouse layer
├── tests/                      # pytest + Spark fixture; fast, offline
└── .github/workflows/          # ci.yml (gate) + release.yml (build-once)
```

`data/` is generated — it is git-ignored. Regenerate it from `seed/` whenever
you need it; the committed generator is what lets a reviewer reproduce exactly
the data this pipeline was built and tested against.

---

## Design decisions

- **Bronze / Silver formats.** Source drops arrive as permissive JSON, every
  column text, so a malformed value lands rather than failing the read. Bronze
  preserves that drop as-is; Silver is typed columnar Parquet. The JSON-to-Parquet
  hop is the only place types and rules get applied, never at read time.
- **Explicit schemas.** `SILVER_PLAYS_SCHEMA` is the single typed contract.
  `SOURCE_PLAYS_SCHEMA` (all-string) and the Silver projection are both derived
  from it, so the read schema and the typed schema cannot drift apart. `users`
  follows the same pattern.
- **Validation and quarantine.** Casts use `try_cast`, which returns null rather
  than raising under Spark's ANSI mode, so a bad value cannot crash the job. A row
  that fails a required-field cast or the `ms_played >= 0` range rule is tagged
  with a single `reject_reason` (first failing rule wins) and written to a
  `quarantine/` path with its raw values intact, so rejects stay inspectable.
- **`plays` idempotency.** Silver is partitioned by `snapshot_date` and written
  with dynamic partition overwrite, so re-running a snapshot rewrites only that
  partition. Dedup uses a `row_number()` window over a total ordering, so the
  surviving row is defined rather than arbitrary; without a full ordering the
  winner could flip under a shuffle and break the re-run guarantee. Provenance is
  deterministic too: `ingested_at` is passed in, not `current_timestamp()`.
- **Event vs ingestion time.** `event_date` is derived from `played_at`, so a late
  event (recorded a day after it happened) is dated to when it happened. It still
  lands physically in its arrival snapshot's partition; `event_date` and
  `snapshot_date` are separate columns by design.
- **`users` SCD2.** The dimension is historised, not overwritten row-for-row. Each
  run unions the incoming snapshot with the current dimension and recomputes
  `valid_from` / `valid_to` / `is_current` with window functions. Versions are
  ordered by `coalesce(updated_at, created_at)`: the source leaves `updated_at`
  null until a row changes, making this the true "effective from" time. A new
  version opens only when a tracked attribute differs from the previous one (a
  `lag` over that ordering), so re-ingesting an unchanged snapshot opens nothing.
  That gate is what makes the SCD2 build idempotent.
- **Why the dimension is a full overwrite.** `users` Silver is rewritten whole
  each run, not partitioned by `snapshot_date` like `plays`. An SCD2 version spans
  a range of snapshots (`valid_from..valid_to`), so it belongs to no single one,
  and the recompute rewrites the whole table anyway. Reading the current dimension
  before overwriting its own path needs a `localCheckpoint` to break lineage,
  otherwise Spark refuses to overwrite a location the query still reads from.
- **Shared vs per-table logic.** `bronze.read_source` and
  `storage.write_partitioned` are the shared skeleton. `tables/plays.py` and
  `tables/users.py` hold the per-table schema, validation, and conform (event-time
  dating for plays, SCD2 for users). Entry-points only parse args and call
  `run_plays` / `run_users`.
