# Seed data — the SonicWave source drop

`generate_seed.py` produces the **raw source** your pipeline ingests: three
daily snapshots (`2026-03-01`, `2026-03-02`, `2026-03-03`) of two tables,
written through a Spark session as permissive JSON.

## Run it

From the repo root, with the module environment active (uv + JDK + PySpark):

```bash
uv run python seed/generate_seed.py            # writes to ./data/source
# or choose a location:
uv run python seed/generate_seed.py --output ./data/source
```

You get, per table, one dated folder per snapshot:

```
data/source/
├── users/  2026-03-01/  2026-03-02/  2026-03-03/
└── plays/  2026-03-01/  2026-03-02/  2026-03-03/
```

**Don't commit `data/`** — it's generated output. Add it to `.gitignore` and
regenerate it whenever you need it.

**Do commit `generate_seed.py`.** The generator *is* your source of truth for
the data: any change you want in the seed — an extra row, a new edge case, a
different snapshot — must be made **in this script**, not by hand-editing the
generated files (those aren't committed and get overwritten on the next run).
The committed generator is what lets a reviewer reproduce exactly the data your
pipeline was built and tested against.

## What's in the data (on purpose)

Everything lands as **text** — a permissive source where types and rules are
*your* job downstream. Every row carries two source timestamps:

- **`created_at`** — when the source row was born (never null).
- **`updated_at`** — when it was last changed, or **null if it has never
  changed** since creation. `updated_at IS NULL` means "still in its original
  state"; a value means a real update happened. Order SCD2 versions by
  `coalesce(updated_at, created_at)`.

Across the three snapshots the data deliberately carries:

- **`users`** — a full snapshot each day. User 3 changes plan tier (T→T+1, its
  `updated_at` flips from null to the change time) then country (T+1→T+2); user
  5 changes country; new users appear at T+1 and T+2. This is what drives your
  **SCD2** history. Plus a duplicate row (**dedup**) and a null-email row
  (**quarantine**) to handle.
- **`plays`** — that day's new events, and a **late-arriving** play: `played_at`
  on an *earlier* day than its `created_at` (recorded late), showing up in a
  later drop. Plus a negative duration, a null user, a duplicate id
  (**dedup**), and an unparseable duration — the malformed ones your Silver
  must **quarantine**, not crash on.

Between them the two tables exercise every pattern the project asks for —
SCD2, late data, partition-overwrite idempotency, dedup, and quarantine — so
there is no separate reference dimension to build.

## It's yours to change

This seed is a starting point, not a fixed input. Add rows, add a table, or
craft a specific edge case you want your pipeline to prove it survives — then
regenerate. Designing the scenario is part of the work.
