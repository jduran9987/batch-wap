# CLAUDE.md — batch_data_generator

Working rules for the mocked source package. Read before changing any file here.

## Refactor rule

The generator's **output structure and defect counts are a contract**, not an
implementation detail. The ingestion jobs and their stress tests depend on the
key set, sequencing, and exact defect counts being stable — but **not** on
specific field values or defect placement, which are random and differ on every
run. When refactoring, preserve the observable output contract (schema, counts,
sequencing, overlap precedence) exactly — the invariants below are the things
that must not change.

> ⚠️ A contract test suite lives at `tests/batch_data_generator/`. It consumes the
> generator's output **in memory** and never touches the sink, so it stays valid
> across a storage-backend change. Run it (and make sure the golden baselines are
> recorded) **before** a refactor, and confirm it still passes after. The docs
> describe the contract; only these tests enforce it.

### Verifying changes

After any change here, run the contract suite from the repo root. All project
commands run through `uv`:

```bash
uv sync                                    # install the package + deps
uv run pytest tests/batch_data_generator   # green = the data contract still holds
```

The golden tests record a baseline on first run, then enforce it. Record the
baseline against the current code **before** refactoring; see
`tests/batch_data_generator/README.md` for the full record-then-enforce workflow
and the adjustment points if the suite fails on import.

## Module ownership

Keep these responsibilities separated; a refactor should not blur them.

- **`generator.py`** — row generation only. Builds records, injects defects,
  applies overlap precedence, yields in chunks. Does **not** validate, build
  dataframes, write to storage, or persist state.
- **`state.py`** — generation state and reservations: sequence/range tracking,
  load/save via plain overwrite. Pure bookkeeping, no I/O to the warehouse.
- **`envelope.py`** — the raw-record shape and its serialization/hashing (the
  on-the-wire/at-rest representation of a generated row).
- **`sink.py`** — writing batches out to the raw table in ClickHouse. The only
  module that touches the warehouse; the storage backend lives behind this
  boundary so the generator stays destination-agnostic. See *ClickHouse sink*
  below for the load-bearing details.
- **`cli.py`** — argument resolution and orchestration; owns run ordering and
  the operational flags.

## Load-bearing invariants

These define the contract. Changing any of them is a behavior change, not a
refactor.

- **Non-deterministic output** — runs are **not** reproducible. The generator
  uses an unseeded RNG (and `Faker` without a fixed seed), so defect placement,
  event types, and messages differ on every run. Only the *structural* contract
  is stable: the key set, sequential ids, exact defect counts, and overlap
  precedence. Do not write tests or downstream consumers that depend on specific
  field values or on which rows carry which defects.
- **Unique sequential keys** — primary keys are unique and sequential across a
  run, continuing from prior state (no gaps, no reuse).
- **Exact defect counts** — each defect knob produces exactly the requested
  number of affected rows. Counts are resolved to integers by the CLI (`ALL` →
  `num_records`) before reaching the generator, which clamps defensively.
- **Overlap precedence** — a single row may carry multiple defects. When several
  target the same field, resolution order is fixed:
  1. duplicate primary key (overrides the id)
  2. schema violation (required field → null)
  3. extra field (adds an unexpected field)
  4. renamed field
  5. missing field (removes the field)

  Later wins: a row flagged both *renamed* and *missing* ends up *missing*.
- **Batched inserts at the sink boundary** — the generator hands records to
  the envelope, which yields rows to the sink; the sink inserts them in
  fixed-size batches (`--batch-size`). The generator itself does not need to
  stream, but the sink must never do row-by-row inserts.
- **State advances only on success** — run ordering is: load state →
  generate → write all batches → save next state **only after** all inserts
  succeed. Any failure mid-write leaves the state file untouched (no partial
  advance).
- **Missing state file yields empty state** — loading a missing state file
  yields empty state (`last_id` 0, so ids start at 1). Saving state is a plain
  in-place overwrite; it is **not** crash-safe, and that is acceptable for this
  workload (state advances only after a fully successful write).

## ClickHouse sink

The sink writes directly to a ClickHouse `MergeTree` table over the HTTP
interface using `clickhouse-connect`. It is **append-only**: there is no update,
delete, or replace path. Treat these as load-bearing:

- **Schema (fixed).** Four columns: `batch_id String`, `data String`,
  `hashed_json String`, `loaded_at DateTime64(6, 'UTC')`. `data` holds the
  generated record as an opaque JSON string, so schema-drift defects never
  change the table schema.
- **Engine + layout.** `ENGINE = MergeTree`, `ORDER BY (loaded_at, batch_id)`,
  `PARTITION BY toYYYYMM(loaded_at)`. Plain `MergeTree` (not `ReplacingMergeTree`)
  is deliberate — duplicates on insert retries are acceptable, and any
  deduplication is the ingestion job's responsibility, not the source's.
- **Table creation, not database creation.** The sink runs
  `CREATE TABLE IF NOT EXISTS` on first write. It never creates the database;
  a missing database surfaces as a ClickHouse error.
- **Batched inserts.** Rows are flushed in fixed-size batches
  (`--batch-size`, default `50_000`). No row-by-row inserts.
- **Retry, not dedup.** Each batch insert is retried with exponential backoff
  on failure. Retries may produce duplicate rows; that is by design.
- **Connection config from CLI + env.** Host, port, user, password come from
  `--ch-*` flags with `CLICKHOUSE_*` env-var fallbacks. The database comes from
  the `database.table` form of `--table`.

## CLI contract

- `--table` must be in `database.table` form; the database must already exist.
- `--batch-size` controls how many rows go in a single insert; must be positive.
- ClickHouse credentials are read from `--ch-host` / `--ch-port` / `--ch-user`
  / `--ch-password`, with `CLICKHOUSE_*` env-var fallbacks. No secrets in code.
