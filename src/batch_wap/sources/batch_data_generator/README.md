# Batch Data Generator

Generates **unstructured** synthetic event records and appends them to a
ClickHouse `MergeTree` table. Each record is stored as an opaque JSON string,
so deliberate data-quality defects (missing fields, extra fields, renamed
fields) are represented naturally in the JSON without ever changing the table
schema. This is useful for exercising downstream audit/validation logic.

Writes are **append-only** and **batched** — never row-by-row, never updates or
deletes.

## ClickHouse table schema

The sink creates the target table on first write if it does not already exist
(the database itself must already exist):

```sql
CREATE TABLE IF NOT EXISTS <database>.<table> (
    batch_id    String,
    data        String,
    hashed_json String,
    loaded_at   DateTime64(6, 'UTC')
) ENGINE = MergeTree
ORDER BY (loaded_at, batch_id)
PARTITION BY toYYYYMM(loaded_at);
```

| column        | type                  | notes                                          |
|---------------|-----------------------|------------------------------------------------|
| `batch_id`    | `String`              | one id (UUID hex) shared by every row in a run |
| `data`        | `String`              | the event record serialised as a JSON string   |
| `hashed_json` | `String`              | SHA-256 hex digest of `data`                   |
| `loaded_at`   | `DateTime64(6, UTC)`  | the script's execution time (microsecond)      |

Group by `batch_id` to collapse a run into one logical job; sort by `loaded_at`
to order jobs chronologically. The ORDER BY and monthly partition match that
access pattern; plain `MergeTree` (not `ReplacingMergeTree`) is deliberate so
retry-induced duplicates are visible rather than silently merged away.

## Event record (inside `data`)

A clean record is a JSON object with four keys:

| key          | notes                                            |
|--------------|--------------------------------------------------|
| `id`         | unique across every run (backed by a state file) |
| `event_type` | one of `event_one`, `event_two`, `event_three`   |
| `event_ts`   | ISO-8601 execution timestamp                     |
| `message`    | a random lorem-ipsum paragraph                   |

## Usage

The package exposes a `batch-data-generator` console command:

```bash
batch-data-generator \
  --total-rows 1000 \
  --table analytics.events \
  --batch-size 50000
```

`--table` must be in `database.table` form. The database must already exist;
the table will be created if missing.

### Local ClickHouse

The repo ships a `docker-compose.yaml` at the project root with a ClickHouse
instance:

```bash
docker compose up -d clickhouse
# create the database the generator will write to
docker compose exec clickhouse clickhouse-client \
  --user batchwap --password batchwap \
  --query "CREATE DATABASE IF NOT EXISTS analytics"
```

Then point the generator at it:

```bash
CLICKHOUSE_USER=batchwap CLICKHOUSE_PASSWORD=batchwap \
  batch-data-generator -n 1000 --table analytics.events
```

## CLI arguments

| argument               | required | default                  | description                                                              |
|------------------------|----------|--------------------------|--------------------------------------------------------------------------|
| `-n`, `--total-rows`   | yes      | —                        | Total number of records to produce.                                      |
| `--table`              | yes      | —                        | Target table as `database.table` (database must exist).                  |
| `--batch-size`         | no       | `50000`                  | Rows per ClickHouse insert.                                              |
| `--ch-host`            | no       | `localhost`              | ClickHouse host (env: `CLICKHOUSE_HOST`).                                |
| `--ch-port`            | no       | `8123`                   | ClickHouse HTTP port (env: `CLICKHOUSE_PORT`).                           |
| `--ch-user`            | no       | `default`                | ClickHouse user (env: `CLICKHOUSE_USER`).                                |
| `--ch-password`        | no       | `""`                     | ClickHouse password (env: `CLICKHOUSE_PASSWORD`).                        |
| `--state-file`         | no       | `.batch_wap_state.json`  | Path to the JSON file that tracks the id sequence.                       |
| `--null-message`       | no       | `0`                      | Records receiving the null-message mutation (an integer or `ALL`).       |
| `--duplicate-id`       | no       | `0`                      | Records receiving the duplicate-id mutation (an integer or `ALL`).       |
| `--extra-user-id`      | no       | `0`                      | Records receiving the extra-user-id mutation (an integer or `ALL`).     |
| `--missing-event-ts`   | no       | `0`                      | Records receiving the missing-event-ts mutation (an integer or `ALL`).  |
| `--rename-event-type`  | no       | `0`                      | Records receiving the rename-event-type mutation (an integer or `ALL`). |

## Configuration

Secrets are never hardcoded. Each `--ch-*` flag falls back to the matching
`CLICKHOUSE_*` environment variable. The recommended workflow is to export the
credentials once per shell session and rely on the defaults:

```bash
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=batchwap
export CLICKHOUSE_PASSWORD=batchwap
```

## Batching and retry behaviour

The sink inserts rows in fixed-size batches (`--batch-size`). Each batch is
retried up to three additional times with exponential backoff on transient
errors. Duplicates produced by a retry are accepted (the engine is plain
`MergeTree`); dedup is the downstream ingestion job's responsibility, not the
source's.

## Mutation flags

Each mutation flag takes an integer (the number of records to affect) or `ALL`
(every record). Mutations are applied as independent random masks, so one record
may receive more than one.

| flag                  | effect on the JSON record                    |
|-----------------------|----------------------------------------------|
| `--null-message`      | sets `"message"` to `null`                   |
| `--duplicate-id`      | reuses an already-allocated `id`             |
| `--extra-user-id`     | adds a `"user_id"` key with a random integer |
| `--missing-event-ts`  | omits the `"event_ts"` key entirely          |
| `--rename-event-type` | renames `"event_type"` to `"type"`           |

Example - 500 rows, half with null messages, every record renamed to `type`:

```bash
batch-data-generator -n 500 --null-message 250 --rename-event-type ALL \
  --table analytics.events
```

## Id uniqueness / state file

Record `id`s never repeat across runs. The last allocated id is stored in a
small JSON state file (`--state-file`, default `./.batch_wap_state.json`) and
advanced after each successful write. Delete the file to restart the sequence.
