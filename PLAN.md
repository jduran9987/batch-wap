# PLAN — WAP v1 staging job (`wap_v1`)

## Context

`SPEC.md` defines the stage-1 WAP staging job: read a 10-minute `loaded_at`
window of raw events, validate each record against the source contract in
`docs/batch-data-generator-findings.md`, quarantine failures, write good
flattened rows to staging, and record one stats row per run. The whole design
hangs on **idempotency via `job_run_id`-per-partition**: `job_run_id` is a pure
function of `window_start`, every target table is `PARTITION BY job_run_id`
(stats is monthly + `ReplacingMergeTree`), and a write means *drop-then-replace
that window's partition*. Reruns and concurrent windows are therefore replay-safe
and crash-healing.

Nothing exists yet under `src/batch_wap/ingestion/wap_v1/` or `tests/wap_v1/`.
This plan builds it bottom-up — pure modules first (each with tests written
alongside), then the I/O seam, then orchestration, then the job-level tests that
the spec enumerates. It does **not** restate the spec; section references below
point into `SPEC.md`.

Two behaviors resolved with the user (beyond the spec text):
- **ids tz policy** — reject naive datetimes (raise `ValueError`); never fall
  back to local tz. (Spec §"Batch window", "Assumptions to confirm" #1.)
- **Failure path** — on any pipeline exception, write a `status='failed'` stats
  row with a **classified stage label** as `error_message`, then re-raise. See
  *Step 8* below.

## Conventions to mirror (from the existing source package)

- `sink.py` pattern: a `_create_table_ddl(...)` builder + module-level
  `COLUMN_NAMES` / `COLUMN_TYPES` lists as the single source of truth for inserts
  → replicate per target table in `schemas.py`.
- `clickhouse-connect` client owned by one module only (the `sink.py` boundary)
  → `clickhouse.py` is the only module here that imports `clickhouse_connect`.
- Test seam: `test_sink.py` uses an in-memory fake client with `time.sleep`
  neutralised; `_contract_api.py` is the stable import adapter. We use the
  spec's `Reader`/`Writer` protocol fakes (no DB client at all).
- House style (root `CLAUDE.md`): module/class/function docstrings everywhere,
  full type annotations, `from __future__ import annotations`, ruff-clean.
- Test dirs are **flat** — no `__init__.py` in `tests/batch_data_generator/`;
  `tests/wap_v1/` follows suit (just `conftest.py` + test modules).
- All commands run through `uv` (never bare `python`/`pip`/`pytest`).

## Step 0 — Scaffolding & dependencies (do first; everything imports these)

- `uv add polars pydantic` (neither is in `pyproject.toml`; `clickhouse-connect`
  is already present).
- Create packages: `src/batch_wap/ingestion/__init__.py` and
  `src/batch_wap/ingestion/wap_v1/__init__.py` (match `sources/__init__.py`,
  which exists). Each gets a one-line module docstring.
- Create `tests/wap_v1/` (flat, no `__init__.py`).

## Step 1 — `ids.py` + `test_ids.py`  (the idempotency lynchpin; pure)

`ids.py`: `derive_job_run_id(window_start: datetime) -> str`.
- Reject naive input: `if window_start.tzinfo is None: raise ValueError(...)`.
- `return "w" + window_start.astimezone(timezone.utc).strftime("%Y%m%d%H%M")`.
- Spec §"Batch window and the `job_run_id` lynchpin".

`test_ids.py` (written alongside):
- `[11:50,12:00) UTC -> "w202606051150"` (spec worked example).
- A non-UTC tz-aware input is normalized to UTC before formatting (same id as
  its UTC equivalent).
- Naive datetime raises `ValueError`.
- Determinism: two calls with the same `window_start` return the same id (the
  property all of idempotency rests on).

## Step 2 — `models.py` + `test_models.py`  (per-row validation; pure)

`models.py`: the strict pydantic model from SPEC §"Validation rules"
(`ConfigDict(strict=True, extra="allow")`, fields `id:int, event_type:str,
event_ts:str, message:str`), plus two pure helpers returning a small result
object (e.g. a frozen dataclass `ValidationResult` with `ok`, `validated:dict`,
`extra_keys:list[str]`, `errors:list[dict]`, `error_count:int`):
- `validate_data(data: dict) -> ValidationResult` — runs the model; on
  `ValidationError` capture `e.errors()` (list of dicts) and the count; on
  success expose validated fields and `model_extra` key names.
- `parse_and_validate(raw_json: str) -> ValidationResult` — `json.loads` first;
  a `JSONDecodeError` becomes a single synthetic parse error (spec: "If `data`
  is not parseable JSON, the row is quarantined with a parse error"); otherwise
  delegate to `validate_data`.

`test_models.py` (alongside) — drives plain dicts/strings, no frame needed.
Cross-check each defect class against the contract findings (§3, §10):
- `null_message` (JSON `null`) → fails (strict non-null string).
- `missing_event_ts` (key absent) → fails (required).
- `rename_event_type` (`event_type` absent, `type` present) → fails required
  **and** `type` shows up as an extra key.
- clean record → ok, no extras.
- `extra_user_id` → ok; `user_id` in `extra_keys`.
- `duplicate_id` (a valid int that happens to collide) → ok (dedup is downstream;
  spec §"How each source defect is handled").
- strict mode: `"id": "5"` (string) → fails (no coercion).
- non-JSON string → parse error result.

## Step 3 — `flatten.py` + `test_flatten.py`  (valid rows → staging frame; pure) [spec test #4]

`flatten.py`: `flatten_valid(good_rows, job_run_id) -> pl.DataFrame` where each
input carries validated fields + source lineage (`batch_id`, `loaded_at`,
`hashed_json`). Produce a Polars frame with **exactly** the staging insert
columns (spec §"Target tables" #1), `processed_at` **omitted** so ClickHouse
applies its `DEFAULT now64(6)`:
`id (Int64), event_type, event_ts, message, hashed_json, batch_id,
source_loaded_at (=source loaded_at), job_run_id`.
Extra keys are dropped from the row (spec §"Flatten step"; §Non-goals). Build
with an explicit Polars schema so dtypes are pinned (notably `id` → `Int64`).

`test_flatten.py` (spec §Tests #4): valid records yield a frame with exactly the
staging columns, correct dtypes (`id` Int64, etc.), `hashed_json` + lineage
carried, extra keys absent.

## Step 4 — `schemas.py`  (DDL + insert metadata; pure, optional light test)

Mirror `sink.py`: one `*_ddl()` builder per target table returning the exact
`CREATE TABLE IF NOT EXISTS` from SPEC §"Target tables" (1 `raw.stg_events`,
2 `quarantine.raw_events`, 3 `statistics.stg_events`), plus per-table
`*_INSERT_COLUMNS` lists (excluding `DEFAULT` columns `processed_at` /
`quarantined_at` / stats defaults) used by `clickhouse.py`. Single source of
truth for column order. Optional unit test (mirrors `test_sink`'s
`_create_table_ddl` assertion): each staging/quarantine DDL contains
`PARTITION BY job_run_id`; stats contains `ReplacingMergeTree` +
`PARTITION BY toYYYYMM(window_start)`.

## Step 5 — `clickhouse.py`  (the only I/O module; protocols + concrete impl)

Defines the test seam used by everything above it:
- `Reader` / `Writer` `Protocol`s exactly as SPEC §"Interfaces" lists.
- `ClickHouseReader.read_raw_events(window_start, window_end)` — half-open
  `[start, end)` SELECT (spec §"Batch window") via **parameterized** query
  (`{start:DateTime64}`, `{end:DateTime64}`), returned as Polars
  (`pl.from_arrow(client.query_arrow(...))`).
- `ClickHouseWriter`:
  - `ensure_tables()` — run the three `CREATE TABLE IF NOT EXISTS` from
    `schemas.py`. **Tables only, never databases**; if a target DB is absent let
    the ClickHouse error surface, wrapped with a clear message naming the missing
    DB (spec §"Job flow" #2, §Non-goals).
  - `replace_staging_partition(job_run_id, df)` / `replace_quarantine_partition(
    job_run_id, df)` — see *partition-replace write path* below.
  - `write_stats(row)` — single `INSERT` into `statistics.stg_events`
    (`ReplacingMergeTree` handles reruns; no drop).
- Concrete reader/writer are **integration-tested only** (spec §"Interfaces":
  the SQL / `DROP PARTITION` / `INSERT` are exercised by integration, not unit
  tests) — consistent with the source package, where `test_sink` covers the
  client wrapper but there is no live-CH test. Not built in this scope.

### Partition-replace write path (load-bearing — get this exactly right)

Both `replace_*_partition` methods implement **unconditional drop, conditional
insert**:

```
ALTER TABLE <tbl> DROP PARTITION {job_run_id:String}   -- ALWAYS, even if df empty
if df.height:                                          -- skip insert only when empty
    client.insert(<tbl>, df.rows(), column_names=<schemas insert cols>)
return df.height
```

Why the drop is unconditional (and the subtle bug it prevents): if a prior
attempt wrote rows to partition `J` and the rerun produces *zero* rows for that
table (e.g. the previously-bad row is now absent, or all rows are good so
quarantine is empty), skipping the drop would leave **stale rows** in `J`.
Dropping always makes the partition contents a pure function of the current run.
Spec's pseudocode (§"Write procedure") shows the insert skipped when `bad_df` is
empty, but the `DROP` is never skipped — preserve that asymmetry.

Other invariants for this path:
- `DROP PARTITION` on a non-existent partition is a ClickHouse no-op (not an
  error), so the first run and every retry are safe — no `IF EXISTS` needed, no
  rollback logic.
- `job_run_id` is bound as a `String` parameter, never string-formatted into
  SQL. (It is `w` + 12 digits by construction, but bind it anyway.)
- `ensure_tables()` runs before any drop/insert.
- Dedup of bad rows on `hashed_json` happens **in `job.py` before** calling
  `replace_quarantine_partition` (so the captured writer input is already
  deduped and the dedup test can assert on it) — not inside the writer.

## Step 6 — `tests/wap_v1/conftest.py`  (fakes; no DB)

- `FakeReader(df)` → `read_raw_events(...)` returns the injected source frame.
- `FakeWriter` → records `ensure_tables()` calls; captures
  `(job_run_id, df)` for each `replace_staging_partition` /
  `replace_quarantine_partition`; captures every `write_stats(row)`; returns
  `df.height` as the inserted count. Optional knobs to make a chosen method
  raise (drives the failure-path test). **Constructs no `clickhouse_connect`
  client** (spec §Tests: "No DB client constructed").
- Source-frame builder helper: given a list of `data` dicts (clean / mutated),
  build a raw-events Polars frame with `batch_id`, `data` (JSON string),
  `hashed_json` (real SHA-256 of the data bytes, matching the source contract
  §8 so dedup keys are realistic), `loaded_at`. Reuse the finding-doc field
  names; consider a tiny constants block echoing `_contract_api.py`
  (`NULL_FIELD`, `MISSING_FIELD`, `RENAMED_TO`, `EXTRA_FIELD`, …) so tests read
  against contract names, not literals.

## Step 7 — `job.py`  (orchestration; ties the pure modules to the writer)

`run(window_start, window_end, reader, writer) -> dict` (returns the stats row).
Order (spec §"Job flow" / §"Write procedure"):
1. `job_run_id = derive_job_run_id(window_start)` (`ids.py`).
2. `run_started_at = datetime.now(UTC)`; `t0 = time.monotonic()`.
3. `writer.ensure_tables()`.
4. `df = reader.read_raw_events(window_start, window_end)`.
5. Validate: iterate `df.iter_rows(named=True)`, call
   `models.parse_and_validate(row["data"])`; split into good (validated fields +
   lineage) and bad (original envelope `batch_id/data/hashed_json/loaded_at` +
   `job_run_id` + `validation_errors` JSON string + `error_count`). Collect
   `unexpected_columns` = sorted distinct `extra_keys` **from valid rows only**
   (spec §"Flatten step", stats field).
6. `good_df = flatten.flatten_valid(good, job_run_id)`;
   `writer.replace_staging_partition(job_run_id, good_df)`.
7. `bad_df = <built frame>.unique(subset=["hashed_json"])`;
   `writer.replace_quarantine_partition(job_run_id, bad_df)`.
8. Build the stats row (window bounds, `rows_read`, `rows_written_staging`,
   `rows_quarantined`, `unexpected_columns`, monotonic `latency_seconds`,
   wall-clock `run_completed_at`, `status='success'`, `error_message=None`);
   `writer.write_stats(row)`.

### Failure handling (per resolved decision)

Wrap steps 3–7 in `try/except`. Track which stage is executing so the except
block can classify (a single `_classify(stage, exc) -> str` helper in `job.py`
mapping to one of: `read_failed`, `validation_failed`, `staging_write_failed`,
`quarantine_write_failed`, `stats_write_failed`, `unknown`). On exception:
- Build a stats row with `status='failed'`, `error_message=<stage label>`,
  `rows_written_staging=0`, `rows_quarantined=0` (don't report unverified
  counts), but real `run_started_at` and `latency_seconds`.
- `write_stats(failed_row)` is **best-effort**: guard it so a second exception
  from the stats write cannot mask the original (catch-and-log around the failed
  stats write).
- **Re-raise the original exception** so the orchestrator's task fails and its
  retry fires.

No rollback / partial-partition cleanup anywhere: a crash between staging- and
quarantine-replace is expected; the retry drops + replaces **both** partitions
and heals it. Stats is `ReplacingMergeTree` keyed on `job_run_id`, so a later
`success` row supersedes the earlier `failed` row.

## Step 8 — Job-level tests (spec's enumerated suite) [spec tests #1–3 + failure]

All inject `FakeReader` + `FakeWriter`, open no DB connection (spec §Tests):
- `test_validation_quarantine.py` — `null_message`, `missing_event_ts`,
  `rename_event_type` rows route to `replace_quarantine_partition`; original
  envelope preserved; non-empty `validation_errors` / `error_count`; nothing in
  staging for them.
- `test_validation_staging.py` — clean rows + an `extra_user_id` row route to
  `replace_staging_partition`; the extra-key row passes and `user_id` appears in
  the stats `unexpected_columns`; none reach quarantine. (Optional, in-scope:
  assert a `duplicate_id` row also reaches staging — documents the deliberate
  non-quarantine.)
- `test_quarantine_dedup.py` — two bad rows sharing one `hashed_json` in a
  window → a single quarantined row in the captured writer frame.
- `test_job_failure_stats.py` (the resolved failure test) — a `FakeWriter`
  whose `replace_staging_partition` raises causes `run()` to call `write_stats`
  once with `status='failed'` and `error_message='staging_write_failed'`, and
  then re-raise.

## Step 9 — `src/batch_wap/ingestion/wap_v1/README.md`

Brief only (spec §README): the high-level step list (read window → validate →
replace quarantine partition with bad rows → replace staging partition with good
flattened rows → write stats) and a short intro to the three tables
(`quarantine.raw_events`, `raw.stg_events`, `statistics.stg_events`) with their
schemas and partitioning. Do not duplicate `SPEC.md`.

## Build order summary

```
0 scaffold + uv add polars pydantic + __init__.py files + tests/wap_v1/
1 ids.py            + test_ids.py
2 models.py         + test_models.py
3 flatten.py        + test_flatten.py            [spec #4]
4 schemas.py        (+ optional DDL assertion test)
5 clickhouse.py     (protocols + concrete impl; concrete = integration-only)
6 conftest.py       (FakeReader / FakeWriter / source-frame builder)
7 job.py            (orchestration + partition-replace order + failure handling)
8 test_validation_quarantine.py / test_validation_staging.py /
  test_quarantine_dedup.py / test_job_failure_stats.py     [spec #1–3 + failure]
9 README.md
```

Pure leaves first so each is unit-tested in isolation; the `Reader`/`Writer`
protocols (Step 5) exist before `conftest`/`job` so fakes and orchestration type
against them; job-level tests come last because the spec specifies them at the
`job.run` seam.

## Verification (run the suite end-to-end)

```bash
uv sync                                  # install polars + pydantic + package
uv run pytest tests/wap_v1 -v            # the new suite (must be green, no DB)
uv run pytest                            # full suite — wap_v1 + batch_data_generator contract intact
uvx ruff check src/batch_wap/ingestion/wap_v1 tests/wap_v1   # ruff-clean per CLAUDE.md
```

Done when (spec §"Done when"): the four spec tests (plus the failure test) pass
with no database connection, `ruff` is clean, the three DDLs match the spec
(staging/quarantine `PARTITION BY job_run_id`, stats monthly +
`ReplacingMergeTree`), and `wap_v1/README.md` exists. The existing
`tests/batch_data_generator` suite must remain green — this work only adds files
and two dependencies, touching nothing in the source package.

> Note: the deliverable is this content saved to repo-root `PLAN.md`.
