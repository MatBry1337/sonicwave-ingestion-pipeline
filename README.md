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

- [x] **Phase 0 — Scaffold**: `src/` package, `pyproject.toml`, CI + release
      workflows, pre-commit, seed copied, green lint/type/test.
- [ ] Phase 1 — `plays` source → Bronze → Silver, one snapshot (validate,
      quarantine, dedup, `event_date`).
- [ ] Phase 2 — `plays` idempotency (dynamic partition overwrite) + late data.
- [ ] Phase 3 — `users` SCD2 dimension (union + recompute, change-gated).
- [ ] Phase 4 — Transform tests (quarantine, dedup, SCD2, late event, schema).
- [ ] Phase 5 — CI green + defended design decisions written up below.

---

## Quickstart

```bash
uv sync                                    # install package + dev tooling
uv run python seed/generate_seed.py        # -> ./data/source/<table>/<date>/*.json

# (Phase 1+) run a table through source -> Bronze -> Silver for one day:
# uv run python -m sonicwave_ingest.entrypoints.ingest_plays \
#     --source ./data/source/plays --snapshot-date 2026-03-01
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
│   └── ...                     # bronze / silver / scd2 / tables / entrypoints (Phase 1+)
├── tests/                      # pytest + Spark fixture; fast, offline
└── .github/workflows/          # ci.yml (gate) + release.yml (build-once)
```

`data/` is generated — it is git-ignored. Regenerate it from `seed/` whenever
you need it; the committed generator is what lets a reviewer reproduce exactly
the data this pipeline was built and tested against.

---

## Design decisions

_To be written as each phase lands — the interesting part of the submission._

- **Bronze / Silver formats** — _why permissive JSON in, typed Parquet out._
- **Explicit schemas** — _the typed Silver `StructType` per table._
- **Validation & quarantine** — _which rules reject a row, and where rejects go._
- **Idempotency** — _`plays`: dynamic partition overwrite by `snapshot_date`;
  `users`: change-gated SCD2 recompute._
- **SCD2 ordering key** — _`coalesce(updated_at, created_at)`, and the guard
  that opens a new version only on a real attribute change._
- **Event vs ingestion time** — _`event_date` from `played_at` so a late event
  is dated when it happened, not when it arrived._
- **Shared vs per-table logic** — _how the common skeleton and the per-table
  specifics are factored._
