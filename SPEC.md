# Spec — WAP v1 Staging Job

_Status: draft for review · Source contract: `docs/batch_data_generator_findings.md` (commit a51a2b1 + corrections)_

## Goal

A batch job that reads a **10-minute window** of raw events produced by
`batch_data_generator` from ClickHouse, validates each record against the source
contract, quarantines records that fail, and writes the good flattened records into a
staging table for a later promotion job. It records one statistics row per run. The
overriding requirement is **idempotency**: failed jobs are retried and past windows are
re-run, including while a different window is being processed concurrently, and every
replay must leave the system in the identical state.

The design achieves this by making **a window own a partition**: all three target tables
are `PARTITION BY job_run_id`, and writing a window means atomically replacing that
window's partition. Idempotency, concurrency-safety, and the downstream batch-validation
goal all fall out of that one decision.

## Where

New package: `src/batch_wap/ingestion/wap_v1/`

```
src/batch_wap/ingestion/wap_v1/
  __init__.py
  ids.py           # job_run_id derivation from window_start (the idempotency lynchpin)
  models.py        # pydantic RawEvent (strict) + per-row validation
  flatten.py       # valid records -> Polars staging frame (pure)
  schemas.py       # CREATE TABLE IF NOT EXISTS DDL for the three target tables
  clickhouse.py    # the only module that touches ClickHouse (read window / ensure / replace-partition / insert)
  job.py           # orchestration: read window -> validate -> replace partitions -> stats
  README.md        # see "README" section below
tests/wap_v1/
  conftest.py      # mock source rows; fake reader + fake writer (no DB)
  test_validation_quarantine.py
  test_validation_staging.py
  test_quarantine_dedup.py
  test_flatten.py
```

(`tests/wap_v1/` mirrors the existing convention where `tests/batch_data_generator/`
matches the package leaf name.)

## Batch window and the `job_run_id` lynchpin

The orchestrator drives the job on a 10-minute cadence and passes only the window bounds:

- `window_start` — inclusive lower bound on `loaded_at`.
- `window_end` — exclusive upper bound (`window_start + 10 minutes`).

**`job_run_id` is derived deterministically from `window_start` inside the job**, not
generated and not supplied by the orchestrator:

```
job_run_id = "w" + window_start.astimezone(UTC).strftime("%Y%m%d%H%M")
# [11:50, 12:00) on 2026-06-05  ->  "w202606051150"
```

This is the single most important property in the design. Because the id is a pure
function of the window, a retrigger of a past window computes the **same** id, lands on
the **same** partition, and therefore replaces its own prior output rather than
duplicating it. If the id were random per run, partition-replace would have nothing to
replace and reruns would duplicate. Everything idempotent rests on this.

The read selects exactly the rows in the half-open interval:

```sql
SELECT batch_id, data, hashed_json, loaded_at
FROM raw.raw_events
WHERE loaded_at >= {window_start} AND loaded_at < {window_end}
```

Half-open `[start, end)` ensures a row sitting exactly on a boundary is never claimed by
two adjacent windows. The source is ordered `(loaded_at, batch_id)` and partitioned by
`toYYYYMM(loaded_at)`, so the range scan is efficient. The source is append-only and
immutable within a past window, so re-reading a window yields the identical rows.

## Source contract (what we validate against)

At-rest source table `raw.raw_events` has four fixed columns: `batch_id` (String),
`data` (String — the event JSON), `hashed_json` (String — SHA-256 of `data`),
`loaded_at` (DateTime64(6,'UTC')). The schema never drifts; all variation lives inside
the `data` JSON.

A clean `data` record has exactly four keys:

| Key | Type | Required | Nullable |
|---|---|---|---|
| `id` | integer | yes | no |
| `event_type` | string (`event_one`/`event_two`/`event_three`) | yes | no |
| `event_ts` | string (ISO-8601 UTC) | yes | no |
| `message` | string | yes | no |

## Validation rules

A pydantic model validates each parsed `data` record:

- **Strict mode** (`ConfigDict(strict=True)`) — no type coercion. `"5"` does not satisfy
  `id: int`; only a JSON integer does.
- **Required fields enforced** — a missing key fails.
- **Type and nullability enforced** — `message` must be a non-null string; JSON `null` fails.
- **No business-rule checks** — no enum membership on `event_type`, no ISO-format check on
  `event_ts`, no ranges. Only structural type / required / nullability.
- **Extra keys allowed and logged** (`extra="allow"`) — unexpected keys do not fail a record;
  their names are collected for stats. Extra keys are **not** written to staging.

```python
from pydantic import BaseModel, ConfigDict

class RawEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow")
    id: int
    event_type: str
    event_ts: str
    message: str
```

If `data` is not parseable JSON, the row is quarantined with a parse error.

### How each source defect is handled

| Defect | Effect on `data` | Outcome |
|---|---|---|
| `null_message` | `message` is `null` | **Quarantine** — non-null string required |
| `missing_event_ts` | `event_ts` key absent | **Quarantine** — required field missing |
| `rename_event_type` | `event_type` absent, `type` present | **Quarantine** — `event_type` required missing; `type` is an extra key |
| `extra_user_id` | extra `user_id` key | **Staging** — valid record; `user_id` logged in `unexpected_columns` |
| `duplicate_id` | `id` duplicates row-0's id | **Staging** — a duplicated id is a valid integer; passes type/required/nullability |
| Duplicate rows (retry) | identical envelope row repeated | **Staging** — passes validation; dedup is out of scope (see Non-goals) |

`duplicate_id` and retry-duplicate rows are deliberately **not** quarantined: schema
validation cannot detect them, and the source contract assigns deduplication to a
downstream consumer.

## Job flow

1. Receive `window_start`, `window_end`; derive `job_run_id` (`ids.py`).
2. Ensure the three target tables exist (`CREATE TABLE IF NOT EXISTS`). Do **not** create
   databases; if a target database is absent, fail with a clear error.
3. Read the windowed slice of `raw.raw_events` into a Polars DataFrame.
4. Validate each row's `data`. Good rows are flattened; bad rows are kept with structured
   error detail; extra-key names are collected.
5. **Replace the staging partition** `job_run_id` with the good rows.
6. Dedup bad rows on `hashed_json`, then **replace the quarantine partition** `job_run_id`.
7. Insert one statistics row.

Polars is the dataframe framework throughout.

## Flatten step

One level deep (the contract is flat). Each valid record becomes one staging row: the four
validated fields, the carried `hashed_json`, lineage (`batch_id`, `source_loaded_at`), and
run metadata (`job_run_id`, `processed_at`). Extra keys are dropped from the row (their
names go to stats only).

## Target tables and schemas

All three live in pre-existing databases. The job creates the tables if missing. All three
are `PARTITION BY job_run_id` so a single window can be replaced or dropped as a unit.

### 1. `raw.stg_events` — staging (one partition per window; dropped after promotion)

```sql
CREATE TABLE IF NOT EXISTS raw.stg_events (
    id               Int64,
    event_type       LowCardinality(String),
    event_ts         String,                      -- validated string; promotion job may parse
    message          String,
    hashed_json      String,                      -- carried from source
    batch_id         String,                      -- lineage
    source_loaded_at DateTime64(6, 'UTC'),        -- lineage
    job_run_id       String,
    processed_at     DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree
PARTITION BY job_run_id
ORDER BY (id);
```

Written via partition-replace (see Write procedure). Each window is its own partition.
Staging partitions are **dropped by the downstream promotion job** after a window is
validated and attached to prod, so staging only ever holds un-promoted windows.

### 2. `quarantine.raw_events` — failed rows (one partition per window; TTL ages them out)

Mirrors the source envelope verbatim (the original row is preserved exactly and can be
re-driven) plus structured error detail.

```sql
CREATE TABLE IF NOT EXISTS quarantine.raw_events (
    batch_id          String,                     -- source envelope, preserved as-is
    data              String,
    hashed_json       String,
    loaded_at         DateTime64(6, 'UTC'),
    job_run_id        String,
    validation_errors String,                      -- JSON array of pydantic error dicts
    error_count       UInt16,
    quarantined_at    DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree
PARTITION BY job_run_id
ORDER BY (hashed_json)
TTL toDateTime(quarantined_at) + INTERVAL 14 DAY;     -- retention; confirm duration
```

"No duplicate bad rows" is enforced two ways: **within a run**, bad rows are deduped on
`hashed_json` in Polars before insert; **across reruns**, replacing the partition discards
the prior attempt's rows entirely. TTL drops whole partitions once their rows age out.

### 3. `statistics.stg_events` — one row per run (coarse partition, kept long-term)

Partitioned monthly (not per-window) so a long history of run stats does not accumulate
thousands of partitions. Idempotency for a single-row-per-run table needs no partition
drop — `ReplacingMergeTree` keyed on `job_run_id` collapses a rerun's duplicate row.

```sql
CREATE TABLE IF NOT EXISTS statistics.stg_events (
    job_run_id           String,
    source_table         String DEFAULT 'raw.raw_events',
    window_start         DateTime64(6, 'UTC'),
    window_end           DateTime64(6, 'UTC'),
    run_started_at       DateTime64(6, 'UTC'),     -- wall clock
    run_completed_at     DateTime64(6, 'UTC'),     -- wall clock
    latency_seconds      Float64,                  -- measured with monotonic clock
    rows_read            UInt64,
    rows_written_staging UInt64,
    rows_quarantined     UInt64,
    unexpected_columns   Array(String),            -- distinct extra-key names seen in valid rows
    status               LowCardinality(String),   -- 'success' | 'failed'
    error_message        Nullable(String),
    created_at           DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (job_run_id);
```

A rerun inserts a fresh row; the latest `created_at` wins on merge. Read with `FINAL`
(or `argMax`) when an immediately-correct latest value is required.

## Write procedure (the idempotent core)

For a run with derived id `J`:

```
good_df, bad_df, extra_cols = validate(read_window(window_start, window_end))

-- staging: replace this window's partition
ALTER TABLE raw.stg_events DROP PARTITION '{J}';        -- clears any partial/prior attempt
INSERT INTO raw.stg_events (...) <good_df>;             -- all rows land in partition J

-- quarantine: dedup within run, then replace this window's partition
bad_df = bad_df.unique(subset=["hashed_json"])
ALTER TABLE quarantine.raw_events DROP PARTITION '{J}';
INSERT INTO quarantine.raw_events (...) <bad_df>;       -- skipped if bad_df is empty

-- stats: ReplacingMergeTree handles rerun; just insert
INSERT INTO statistics.stg_events (...) <one row>;
```

Drop-then-insert is idempotent because a retry simply redoes the drop. It is correct to
heal a crashed prior attempt: whatever partial state the crash left in `J` is discarded by
the drop and replaced with the recomputed contents, so the window ends identical no matter
how many times it runs.

There is a brief moment between `DROP` and `INSERT` where partition `J` is empty. This is
safe because nothing reads a staging partition mid-write — the promotion job acts on a
window only after this job has completed (signalled by the `success` stats row /
orchestrator dependency), never by polling staging. If a reader ever must hit staging
during a write, the atomic upgrade is to insert into a **per-run** scratch table
(`raw.stg_events_incoming_{J}`) and `REPLACE PARTITION '{J}' FROM` it — a single metadata
swap. Not the default; only adopt with per-run scratch names so concurrent runs don't share
a scratch table.

## Worked example: retrigger during a concurrent run

12:00 — the [11:50, 12:00) run (`J=w202606051150`, "w1150") reads 10 rows, 9 good / 1 bad,
and **crashes after writing staging but before quarantine** — leaving inconsistent state.

12:30 — two runs proceed at once:

- **w1220** (live [12:20, 12:30), 8 good rows): replaces staging partition `w1220` (8 rows),
  drops empty quarantine partition `w1220`, inserts its stats row.
- **w1150** (retrigger): reads the identical 10 source rows, drops + reinserts staging
  partition `w1150` (9 rows — discarding the crash's partial write), dedups + drops +
  reinserts quarantine partition `w1150` (1 row — discarding the crash's leftover), inserts
  its stats row.

The two runs touch **physically disjoint partitions** (`w1150` vs `w1220`), so they cannot
interfere regardless of interleaving — the exact case a `TRUNCATE`-based design would
corrupt. Final state is deterministic: staging holds `w1150` (9) and `w1220` (8);
quarantine holds `w1150` (1); stats has one success row for each. Re-run w1150 again and
nothing changes.

## Idempotency summary

- Unit of work = a 10-minute `loaded_at` window. `job_run_id` is derived from `window_start`,
  so reruns target the same partitions.
- Source rows for a past window are immutable, so a rerun reads identical input.
- Each table's window-partition is replaced wholesale (staging/quarantine) or
  replace-keyed (stats), so output is a pure function of the window — replay-safe and
  crash-healing.
- Concurrent runs operate on disjoint partitions and cannot collide.

## Partition lifecycle and cost

One partition per 10-minute window is ~144/day, so the design **requires** a lifecycle:

- **Staging** — partitions dropped by the promotion job right after promotion, so only
  un-promoted windows are ever live (normally a handful).
- **Quarantine** — TTL ages partitions out (default 14 days → ~2,000 live partitions;
  partition count ≈ 144 × retention_days). If long retention is needed, switch quarantine
  to monthly partitioning + `ReplacingMergeTree(quarantined_at)` keyed on
  `(job_run_id, hashed_json)` like stats, trading atomic per-window drop for eventual-merge
  dedup.
- **Stats** — monthly partitions, so years of history stay in a few dozen partitions.

The rule behind all three: keep the number of *live* partitions modest. The cleanup is not
optional housekeeping — it is what keeps the per-window grain inside what ClickHouse handles
well.

## How this enables the downstream goal

Because the partition **is** the batch, the promotion job (separate, out of scope here) can:

1. Run **dataset-level** validation against one window: `... WHERE job_run_id = '{J}'`
   (row counts, distinct-id ratios, null ratios — batch-level, not record-level).
2. If it passes, `ATTACH`/`REPLACE PARTITION '{J}'` into the prod table — a near-instant
   metadata move, given prod and staging share partition key / `ORDER BY` / columns.
3. `DROP PARTITION '{J}'` from staging.

Promotion is thus another atomic partition op on a unit this job already isolated.

## Interfaces (the test seam)

`clickhouse.py` is the only I/O module, behind a small protocol so tests never touch a DB:

```python
class Reader(Protocol):
    def read_raw_events(self, window_start: datetime, window_end: datetime) -> pl.DataFrame: ...

class Writer(Protocol):
    def ensure_tables(self) -> None: ...
    def replace_staging_partition(self, job_run_id: str, df: pl.DataFrame) -> int: ...
    def replace_quarantine_partition(self, job_run_id: str, df: pl.DataFrame) -> int: ...
    def write_stats(self, row: dict) -> None: ...
```

`job.run(window_start, window_end, reader, writer)` derives `job_run_id`, then orchestrates
read → validate → replace partitions → stats. Validation (`models.py`), flatten
(`flatten.py`), and id derivation (`ids.py`) are pure functions. The window-filter SQL and
the `DROP PARTITION` / `INSERT` live in `clickhouse.py` and are exercised by integration,
not unit, tests.

## README

Create `src/batch_wap/ingestion/wap_v1/README.md` with a single brief section: the
high-level steps (read window → validate → replace quarantine partition with bad rows →
replace staging partition with good flattened rows → write stats), and a short introduction
to the three tables (`quarantine.raw_events`, `raw.stg_events`, `statistics.stg_events`)
with their schemas and partitioning. A concise overview only — do not duplicate this spec.

## Tests (mock data, no database writes)

All tests inject a fake `Reader` returning a mock Polars frame and a fake `Writer` that
captures what it was handed. No ClickHouse connection is opened.

1. **`test_validation_quarantine.py`** — rows with `null_message`, `missing_event_ts`, and
   `rename_event_type` are routed to `replace_quarantine_partition`, original envelope
   preserved, non-empty `validation_errors` / `error_count`. No DB client constructed.
2. **`test_validation_staging.py`** — clean rows plus an `extra_user_id` row are routed to
   `replace_staging_partition`; the `extra_user_id` row passes and `user_id` appears in
   `unexpected_columns`; none reach quarantine.
3. **`test_quarantine_dedup.py`** — two bad rows with the same `hashed_json` in one window
   result in a single quarantined row (covers the "no duplicate bad rows" requirement).
4. **`test_flatten.py`** — valid records yield a Polars frame with exactly the staging
   columns, correct types (`id` Int64, etc.), `hashed_json` and lineage carried, extra keys
   dropped.

Optional (within scope): a test asserting a `duplicate_id` row reaches staging (documents the
deliberate non-quarantine of duplicate ids).

## Non-goals

- No deduplication of duplicate `id`s or retry-duplicate rows — downstream's job.
- No record-level business validation; dataset-level validation and **promotion to prod
  (including dropping staging partitions) are a separate downstream job**.
- No database creation; only table creation.
- No writing of unexpected source columns into staging.
- No backfill/live distinction (windowing is purely by `loaded_at`).

## Done when

- The three tables exist (databases assumed present), each `PARTITION BY job_run_id`
  (stats monthly-partitioned) per the DDL above.
- A run reads exactly `[window_start, window_end)`, derives `job_run_id` from `window_start`,
  quarantines the three failing defect classes (deduped on `hashed_json`), replaces the
  staging partition with good flattened rows carrying `hashed_json` + lineage, and writes one
  stats row with window bounds, counts, monotonic latency, and the distinct unexpected-column
  list.
- Re-running a past window — including while another window runs concurrently — leaves all
  three tables in identical state and never corrupts the other window.
- The four tests pass without any database connection.
- `src/batch_wap/ingestion/wap_v1/README.md` exists with the brief job section described.

## Assumptions to confirm

1. `job_run_id` is derived **in the job** from `window_start` as `"w" + %Y%m%d%H%M` (UTC);
   the orchestrator passes only the window bounds.
2. `event_ts` stored as String in staging (faithful to validated type); promotion job parses.
3. Quarantine retention is 14 days (partition count ≈ 144 × retention_days). If longer
   retention is wanted, switch quarantine to monthly partition + `ReplacingMergeTree` like
   stats.
4. Stats partitioned monthly with `ReplacingMergeTree` (not per-window) to bound partition
   count for long-kept history.
5. Window convention is half-open `[window_start, window_end)`.
6. Promotion, dataset-level validation, and dropping promoted staging partitions are a
   separate downstream job; this job only writes staging partitions.
