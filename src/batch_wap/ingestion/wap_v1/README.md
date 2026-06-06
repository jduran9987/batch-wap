# WAP v1 — Staging Job

Reads a 10-minute window of raw events from ClickHouse, validates each record
structurally, and routes rows to three target tables.  See `SPEC.md` at the
repo root for the full design rationale.

## Job steps

1. **Read window** — select rows from `raw.raw_events` where
   `loaded_at ∈ [window_start, window_end)`.
2. **Validate** — parse each row's `data` JSON and check it against the
   `RawEvent` pydantic model (strict mode; no coercion).  Split into good and
   bad rows; collect unexpected extra-key names from valid rows for stats.
3. **Replace staging partition** — flatten valid rows (drop extra keys; carry
   lineage), then drop and replace the staging partition `job_run_id`.
4. **Replace quarantine partition** — dedup bad rows on `hashed_json`, then
   drop and replace the quarantine partition `job_run_id` with the bad rows.
5. **Write stats** — insert one row into the statistics table recording window
   bounds, row counts, latency, unexpected columns, and run status.

`job_run_id` is derived deterministically from `window_start` as
`"w" + strftime("%Y%m%d%H%M", window_start UTC)`.  Because the id is a pure
function of the window, any retrigger targets the same partition and replaces
its own prior output.

## Target tables

### `raw.stg_events` — staging

Holds validated, flattened events for one window per partition.  Downstream
promotion drops these partitions after attaching them to the production table.

| Column | Type | Notes |
|---|---|---|
| `id` | Int64 | From source |
| `event_type` | LowCardinality(String) | From source |
| `event_ts` | String | Validated string; promotion job may parse |
| `message` | String | From source |
| `hashed_json` | String | Carried from source envelope |
| `batch_id` | String | Lineage |
| `source_loaded_at` | DateTime64(6,'UTC') | Lineage |
| `job_run_id` | String | Partition key |
| `processed_at` | DateTime64(6,'UTC') | DEFAULT now64(6) |

Engine: `MergeTree`, `PARTITION BY job_run_id`, `ORDER BY (id)`.

### `quarantine.raw_events` — failed rows

Preserves the original source envelope verbatim plus structured validation
errors.  TTL ages partitions out after 14 days.

| Column | Type | Notes |
|---|---|---|
| `batch_id` | String | Source envelope |
| `data` | String | Source envelope |
| `hashed_json` | String | Source envelope; dedup key within a run |
| `loaded_at` | DateTime64(6,'UTC') | Source envelope |
| `job_run_id` | String | Partition key |
| `validation_errors` | String | JSON array of pydantic error dicts |
| `error_count` | UInt16 | `len(validation_errors)` |
| `quarantined_at` | DateTime64(6,'UTC') | DEFAULT now64(6) |

Engine: `MergeTree`, `PARTITION BY job_run_id`, `ORDER BY (hashed_json)`,
`TTL toDateTime(quarantined_at) + INTERVAL 14 DAY`.

### `statistics.stg_events` — one row per run

Monthly-partitioned with `ReplacingMergeTree` keyed on `job_run_id` so a
rerun's row supersedes the prior one on merge.

| Column | Type | Notes |
|---|---|---|
| `job_run_id` | String | Dedup / order key |
| `source_table` | String | DEFAULT `'raw.raw_events'` |
| `window_start` | DateTime64(6,'UTC') | — |
| `window_end` | DateTime64(6,'UTC') | — |
| `run_started_at` | DateTime64(6,'UTC') | Wall clock |
| `run_completed_at` | DateTime64(6,'UTC') | Wall clock |
| `latency_seconds` | Float64 | Monotonic clock |
| `rows_read` | UInt64 | — |
| `rows_written_staging` | UInt64 | — |
| `rows_quarantined` | UInt64 | Post-dedup count |
| `unexpected_columns` | Array(String) | Distinct extra-key names from valid rows |
| `status` | LowCardinality(String) | `'success'` or `'failed'` |
| `error_message` | Nullable(String) | Stage label on failure; NULL on success |
| `created_at` | DateTime64(6,'UTC') | DEFAULT now64(6); ReplacingMergeTree version column |

Engine: `ReplacingMergeTree(created_at)`,
`PARTITION BY toYYYYMM(window_start)`, `ORDER BY (job_run_id)`.
