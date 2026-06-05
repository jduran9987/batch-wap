# batch_data_generator — Findings
_Last updated: 2026-06-05 · against commit a51a2b1 (main), plus the
determinism/state-claim corrections and added sink/clamping/timestamp tests
described below._

---

## Output Contract & Schema

### 1. ClickHouse table schema (the at-rest envelope)

Every row written to ClickHouse has exactly four columns. The schema is fixed and
never changes regardless of what mutations are active — schema-drift defects live
inside the `data` JSON string, not in the column layout.

| Column | ClickHouse type | Nullable | Meaning |
|---|---|---|---|
| `batch_id` | `String` | No | UUID hex (32 lowercase hex chars, no dashes) shared by **every row in a single run** |
| `data` | `String` | No | The event record serialised as a JSON string |
| `hashed_json` | `String` | No | SHA-256 hex digest of the exact bytes in `data` |
| `loaded_at` | `DateTime64(6, 'UTC')` | No | UTC timestamp of script invocation, microsecond precision |

Sources: `sink.py` lines 41-43, `envelope.py` lines 1-10, `README.md` lines 17-26.

**Engine and layout** (`sink.py` lines 55-64):

```sql
CREATE TABLE IF NOT EXISTS `<database>`.`<table>` (
    batch_id    String,
    data        String,
    hashed_json String,
    loaded_at   DateTime64(6, 'UTC')
) ENGINE = MergeTree
ORDER BY (loaded_at, batch_id)
PARTITION BY toYYYYMM(loaded_at)
```

Plain `MergeTree` (not `ReplacingMergeTree`) is intentional. Retry-induced duplicate
rows are never merged away; deduplication is the downstream ingestion job's
responsibility.

### 2. Event record schema (inside `data`)

A **clean** record is a JSON object with exactly four keys:

| Key | JSON type | Nullable | Meaning |
|---|---|---|---|
| `id` | integer | No | Globally unique sequential identifier |
| `event_type` | string | No | One of `"event_one"`, `"event_two"`, `"event_three"` |
| `event_ts` | string | No | ISO-8601 UTC timestamp of script invocation (same value for every record in a run) |
| `message` | string | No | Random lorem-ipsum paragraph |

Sources: `generator.py` lines 22, 79, 81-84, 94-100.

The clean-record shape is test-verified by `test_clean_run_has_canonical_shape`
(`test_generation_contract.py` line 18).

### 3. Mutation defects — per-field effects

Each mutation flag is independently applied as a random mask. A single record may
carry multiple mutations simultaneously. The five mutations and their exact effects
on the JSON record are:

| Mutation flag | CLI argument | Effect on `data` JSON |
|---|---|---|
| `null_message` | `--null-message` | Sets `"message"` to JSON `null` |
| `duplicate_id` | `--duplicate-id` | Replaces `"id"` with the `id` of **row index 0** |
| `extra_user_id` | `--extra-user-id` | Adds `"user_id"` key whose value is a random integer in `[1, 10_000_000]` |
| `missing_event_ts` | `--missing-event-ts` | Omits the `"event_ts"` key entirely |
| `rename_event_type` | `--rename-event-type` | Renames `"event_type"` to `"type"` (old key is removed) |

Sources: `generator.py` lines 86-107.

#### Overlap precedence

When multiple mutations target the same field, application order in the loop
determines the final state. Within the loop body (`generator.py` lines 94-107)
the order is:

1. `id` assignment / duplicate (line 96)
2. `null_message` (line 99)
3. `rename_event_type` — replaces `"event_type"` with `"type"` (lines 101-102)
4. `missing_event_ts` — removes `"event_ts"` if present (lines 103-104)
5. `extra_user_id` — adds `"user_id"` (lines 105-106)

Because `rename_event_type` runs before `missing_event_ts`, a record flagged for
both rename and missing ends up with `"event_ts"` removed but `"event_type"`
already renamed to `"type"` — the rename is visible. The CLAUDE.md states "later
wins; a row flagged both _renamed_ and _missing_ ends up _missing_" for the
missing-event-ts/rename-event-type pair, but `missing_event_ts` targets `event_ts`,
not `event_type`, so there is no actual conflict between those two mutations.
The ordering description in CLAUDE.md describes precedence when mutations touch the
same field; for mutations on distinct fields they accumulate independently.

#### `duplicate_id` collision target

Row index 0 is always canonical: `_select` for duplicates draws only from
`range(1, total)` (`generator.py` line 88). Every duplicate-flagged row
receives `ids[0]`, the id assigned to row 0. This means K duplicate-flagged rows
produce K+1 occurrences of the row-0 id and reduce the unique-id count by exactly K.

Test-verified by `test_duplicate_id_collides_with_first_row`
(`test_generation_contract.py` line 85).

### 4. Exact defect counts

Each mutation produces **exactly** the requested number of affected rows, capped
at `total` when the count exceeds it. The `_select` helper clamps at population
size (`generator.py` line 57: `min(count, len(population))`). The CLI resolves
`"ALL"` to `total` before passing values to `MutationCounts` (`cli.py` lines 61-71).

Test-verified:

| Invariant | Test |
|---|---|
| Exact null-message count | `test_exact_null_message_count` |
| Exact extra-user-id count | `test_exact_extra_field_count` |
| Exact missing-event-ts count | `test_exact_missing_event_ts_count` |
| Exact rename-event-type count | `test_exact_rename_event_type_count` |
| Count > total clamps to total | `test_mutations_clamp_at_total` |

The duplicate-id **clamping edge** is now test-verified by
`test_duplicate_id_clamps_at_population_size` (`test_generation_contract.py`): with
`duplicate_id` far above `total`, the count clamps at the candidate-pool size
`total - 1` (row 0 stays canonical), so every row collapses onto row 0's id —
`total` occurrences and a single unique id.

### 5. Id uniqueness and sequencing

- Ids are sequential integers starting at `start_id + 1` through `start_id + total`
  (`generator.py` line 81).
- Ids are unique within a run on a clean (no-duplicate-mutation) run.
- Across consecutive runs: the state file persists `last_id = start_id + total`;
  the next run starts at `last_id + 1`, so the global sequence has no gaps and no
  reuse (`cli.py` line 190).
- Row 0 is always allocated the lowest id in a run (`ids[0]` = `start_id + 1`).

Test-verified by:
- `test_keys_unique_on_clean_run`
- `test_keys_are_sequential_from_start_id`
- `test_keys_do_not_repeat_across_consecutive_runs`
- `test_state_drives_next_run_start` (state-file integration)

### 6. `event_ts` and `loaded_at` — same timestamp

Both `event_ts` (inside `data`) and `loaded_at` (envelope column) are set to the
same value: `datetime.now(timezone.utc)` captured once at the top of `main()`
(`cli.py` line 169) and passed as `run_ts` to both `generate_records` and
`pack_rows`. Every record in a run shares the same `event_ts` and `loaded_at`.

Test-verified by `test_event_ts_matches_loaded_at_and_is_uniform`
(`test_generation_contract.py`): for a clean run, every record's `event_ts` equals
`loaded_at.isoformat()` and all rows share the same value.

### 7. `batch_id` scope

`batch_id` is a UUID hex string (`uuid.uuid4().hex`) generated once per invocation
(`cli.py` line 170) and attached to every row via `pack_rows`. All rows produced in
a single run share the same `batch_id`. Different runs produce different (random)
`batch_id` values.

Test-verified by `test_pack_rows_emits_envelope_shape` which checks that all five
rows share the supplied `batch_id`.

### 8. `hashed_json` integrity

`hashed_json` is the SHA-256 hex digest of the UTF-8 encoded `data` string
(`envelope.py` line 37). The digest is computed from the exact serialised bytes.

Test-verified by `test_pack_rows_emits_envelope_shape`.

### 9. Production modes and volumes

| Axis | Controlled by | Range / values |
|---|---|---|
| Total rows | `--total-rows` / `-n` (required) | Any positive integer |
| Batch size | `--batch-size` (default 50,000) | Any positive integer; must be > 0 |
| Null messages | `--null-message` (default 0) | `0` to `total` or `ALL` |
| Duplicate ids | `--duplicate-id` (default 0) | `0` to `total` or `ALL` |
| Extra user-id | `--extra-user-id` (default 0) | `0` to `total` or `ALL` |
| Missing event-ts | `--missing-event-ts` (default 0) | `0` to `total` or `ALL` |
| Renamed event-type | `--rename-event-type` (default 0) | `0` to `total` or `ALL` |

There is no concept of "backfill vs. live" mode — every run is a single
append-only batch. Volume is entirely determined by `--total-rows`.

### 10. Data-quality issues a consumer must defend against

| Issue | Trigger condition | Effect in `data` |
|---|---|---|
| Null `message` field | `--null-message` > 0 | `"message": null` in the JSON |
| Duplicate `id` | `--duplicate-id` > 0 | Multiple rows share the row-0 id |
| Extra `user_id` field | `--extra-user-id` > 0 | JSON contains unexpected `"user_id"` key |
| Missing `event_ts` field | `--missing-event-ts` > 0 | `"event_ts"` key absent from JSON |
| Renamed `event_type` → `type` | `--rename-event-type` > 0 | `"event_type"` absent; `"type"` present instead |
| Duplicate rows (retry) | Transient ClickHouse insert failure | Same `(batch_id, data, hashed_json, loaded_at)` row may appear more than once |
| Multiple defects on one row | Any two or more mutation flags > 0 | A single record may exhibit any combination of the above |

### 11. Schema drift

Schema drift is entirely controlled and enumerated. The table schema (`batch_id`,
`data`, `hashed_json`, `loaded_at`) never changes. Within the `data` JSON string,
the possible deviations from the clean four-key shape are:

- `"message"` may be `null` instead of a string.
- `"event_ts"` may be absent.
- `"event_type"` may be absent, replaced by `"type"`.
- `"user_id"` (integer) may be present as an extra key.
- `"id"` may duplicate another row's id.

No other schema changes occur. There is no version mechanism — the schema is
defined by the code, not by a versioned descriptor.

---

## Code Logic & Internals

### 1. Generation flow (end-to-end)

```
cli.main()
  │
  ├─ _parse_args()           → Namespace with all flags
  ├─ _split_table()          → (database, table)
  ├─ MutationCounts(...)     → resolved integer counts
  ├─ datetime.now(UTC)       → run_ts  (shared for event_ts and loaded_at)
  ├─ uuid4().hex             → batch_id
  ├─ read_last_id(state_file) → start_id
  │
  ├─ generate_records(mutations, start_id, run_ts)   [generator.py]
  │     → list[Record]  (plain Python dicts, all in memory)
  │
  ├─ pack_rows(records, batch_id, run_ts)            [envelope.py]
  │     → Iterator[tuple[str, str, str, datetime]]
  │
  ├─ ClickHouseSink.write(rows)                      [sink.py]
  │     ├─ _ensure_table()   (CREATE TABLE IF NOT EXISTS)
  │     └─ for each batch:
  │           _insert_with_retry(batch)
  │              ↳ up to 4 attempts with exponential backoff (1s, 2s, 4s)
  │
  └─ write_last_id(state_file, start_id + total)     [state.py]
       (only reached if sink.write() does not raise)
```

Sources: `cli.py` lines 150-194.

### 2. Module ownership and interactions

**`generator.py`** — Pure data generation. Builds all records in memory as a
`list[Record]`. Uses `random.Random()` (no fixed seed; see note below) and `Faker`.
No imports from other package modules. No I/O.

**`envelope.py`** — Serialisation. Iterates `list[Record]` from `generator.py`,
JSON-serialises each, computes SHA-256, yields 4-tuples. No I/O.

**`state.py`** — Id sequence persistence. Two pure functions: `read_last_id` and
`write_last_id`. Uses `Path.read_text` / `Path.write_text` directly (no temp file,
no atomic rename — the write is not crash-safe at the filesystem level, though the
CLI only calls it after all inserts succeed). No other imports from the package.

**`sink.py`** — ClickHouse I/O. `ClickHouseSink` class connects via
`clickhouse-connect` HTTP. Owns `CREATE TABLE IF NOT EXISTS`, batching (`_chunked`),
and retry logic. The only module that touches the warehouse.

**`cli.py`** — Orchestration. Owns argument parsing (`_parse_args`), `"ALL"`
resolution (`_resolve`), table-name splitting (`_split_table`), and the
generate → pack → write → persist-state sequence. The `main()` function is the
entry point registered as the `batch-data-generator` console script.

### 3. Non-determinism note

`generator.py` constructs `random.Random()` without a seed (`generator.py`
line 77), and `Faker()` is likewise unseeded. The mutation masks (which rows
receive which defects), the chosen event types, and the messages are therefore
random and differ between runs with the same parameters. The public API
(`generate_records`) accepts no `seed` parameter and there is no way to pass one
through the CLI — runs are **not** deterministic across invocations, by design.

The package CLAUDE.md previously claimed "for a fixed seed, the generated data is
reproducible." That claim was **removed**: the Refactor rule and the load-bearing
invariant now state explicitly that output is non-deterministic and only the
*structural* contract (key set, sequential ids, exact defect counts, overlap
precedence) is stable. The contract tests pass because they assert counts and
structure, not specific row positions or values.

### 4. State file details

- Format: `{"last_id": <int>}` (`state.py` lines 26, 36).
- Default path: `.batch_wap_state.json` in the working directory (`cli.py` line 145).
- Missing file returns `0` (ids start at 1 on first run).
- `write_last_id` uses `Path.write_text` — a simple in-place overwrite with no
  atomic rename. If the process is killed after all inserts succeed but before
  `write_last_id` completes, the state file may be partially written or stale.
- This is **intentional and accepted** for this workload. The package CLAUDE.md
  previously listed "atomic state writes" as a load-bearing invariant; that claim
  was **removed**. The invariant now states only that a missing state file yields
  empty state and that the write is a plain, non-crash-safe overwrite. Crash
  safety is explicitly out of scope.

### 5. Retry behaviour

`_insert_with_retry` attempts up to `max_retries + 1` total times (default: 4
attempts). Backoff is exponential: sleep `retry_backoff_seconds * 2^attempt` before
each retry (`sink.py` lines 147-165). The base is `1.0` second (default).
If all attempts fail, the last exception is re-raised; `write_last_id` is never
called, so the state file is not advanced.

### 6. Test suite

**`_contract_api.py`** — Adapter layer. Exposes stable symbolic names for field
constants and wires `run()` as a convenience wrapper around `generate_records`.
Test modules import only from this file; it is the only file with direct API
assumptions.

**`test_generation_contract.py`** — 13 tests covering:
- Total row count
- Clean-run canonical shape (4 keys)
- Unique keys on clean run
- Sequential ids from `start_id`
- No id reuse across consecutive runs (via start_id passing, not state file)
- Exact count for each of the four non-duplicate mutations
- Duplicate-id collision target and count arithmetic
- Duplicate-id clamping at the candidate-pool size `total - 1`
  (`test_duplicate_id_clamps_at_population_size`)
- `event_ts` equals `loaded_at.isoformat()` and is uniform across all rows
  (`test_event_ts_matches_loaded_at_and_is_uniform`)
- Clamping when mutation count > total
- Envelope shape and hash correctness via `pack_rows`

**`test_state_contract.py`** — 4 tests covering:
- Missing file → 0
- Write-then-read round-trip
- Overwrite replaces previous value
- Two runs chained through state file produce contiguous ids 1..200

**`test_sink.py`** — 9 tests exercising `sink.py` directly against an in-memory
fake client (with `time.sleep` neutralised), covering:
- `_chunked` fixed-size batching and the empty-input case
- `_create_table_ddl` matches the four-column `MergeTree` envelope schema
- Non-positive `batch_size` rejected before any connection
- `write` creates the table once, then inserts in correctly sized batches
- Inserts carry the declared `COLUMN_NAMES` / `COLUMN_TYPES` and target table/db
- Retry with exponential backoff (fails twice, succeeds, sleeps `[1.0, 2.0]`)
- Last error propagates after retries are exhausted; nothing is inserted
- `close` closes the underlying client

### 7. Gaps and untested behavior

**Resolved since the prior findings:**

| Former gap | Resolution |
|---|---|
| No seed / non-determinism | CLAUDE.md no longer claims determinism. The Refactor rule and load-bearing invariant now state output is non-deterministic; only the structural contract is stable. No seed parameter exists, by design. |
| `duplicate_id` clamping | Covered by `test_duplicate_id_clamps_at_population_size`. |
| `event_ts` == `loaded_at` | Covered by `test_event_ts_matches_loaded_at_and_is_uniform`. |
| State file atomicity | CLAUDE.md no longer claims atomic state writes; the plain overwrite is accepted as out of scope. (Not a tested behavior — there is nothing to enforce.) |
| Sink tests | `sink.py` now covered by `test_sink.py`: batching, `CREATE TABLE IF NOT EXISTS`, column metadata, retry backoff, and the `batch_size <= 0` guard. |

**Still open:**

| Gap | Risk |
|---|---|
| `batch_id` uniqueness across runs | No test asserts that two calls produce different `batch_id` values. |
| Multiple mutations on one row | No test asserts the combined state of a record that receives two or more mutations simultaneously. |
| `user_id` range | The extra `user_id` value is `rng.randint(1, 10_000_000)`. No test asserts that this range holds. |
| `event_type` values | No test asserts that `event_type` is always one of the three allowed values (`event_one`, `event_two`, `event_three`). |
| CLI argument resolution | `_int_or_all`, `_resolve`, and `_split_table` are not directly tested. |
| Sink integration | `test_sink.py` uses an in-memory fake client; there is no test against a real ClickHouse instance (DDL execution, type coercion, partitioning). |
