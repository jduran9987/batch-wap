# WAP v1 — Staging Job

Reads a 10-minute window of raw events from ClickHouse, validates each record
structurally, and routes rows to three target tables.

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

## Assumptions

- Source data is append-only: every row produced by the source is a new
  record. There are no updates to existing records, so the job does not
  attempt to detect or merge changes to previously ingested rows.

## CLI arguments

Run via `uv run wap-v1-staging`. Each `--*-table` argument takes a fully
qualified `database.table` name, split into separate database/table values for
the ClickHouse client.

| Argument | Description |
|---|---|
| `--window-start` | Inclusive window start (timezone-aware ISO-8601). Required. |
| `--window-end` | Exclusive window end (timezone-aware ISO-8601). Required. |
| `--source-table` | Source table to read raw events from, as `database.table`. Default `raw.raw_events` (env `WAP_SOURCE_TABLE`). |
| `--stg-table` | Staging table to write validated rows to, as `database.table`. Default `raw.stg_events` (env `WAP_STG_TABLE`). |
| `--quarantine-table` | Quarantine table to write failed rows to, as `database.table`. Default `quarantine.raw_events` (env `WAP_QUARANTINE_TABLE`). |
| `--statistics-table` | Statistics table to write the per-run summary row to, as `database.table`. Default `statistics.stg_events` (env `WAP_STATISTICS_TABLE`). |
| `--ch-host` | ClickHouse host. Default `localhost` (env `CLICKHOUSE_HOST`). |
| `--ch-port` | ClickHouse HTTP port. Default `8123` (env `CLICKHOUSE_PORT`). |
| `--ch-user` | ClickHouse user. Default `default` (env `CLICKHOUSE_USER`). |
| `--ch-password` | ClickHouse password (env `CLICKHOUSE_PASSWORD`). |

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
