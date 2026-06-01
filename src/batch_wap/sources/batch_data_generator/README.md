# Batch Data Generator

Generates **unstructured** synthetic event records and appends them to an
Iceberg table (AWS Glue catalog + S3 warehouse). Each record is stored as an
opaque JSON string, so deliberate data-quality defects (missing fields, extra
fields, renamed fields) are represented naturally in the JSON without ever
changing the table schema. This is useful for exercising downstream
audit/validation logic.

## Iceberg table schema

The table is fixed at four columns and never evolves:

| column        | type            | notes                                          |
|---------------|-----------------|------------------------------------------------|
| `batch_id`    | string          | one id (UUID hex) shared by every row in a run |
| `data`        | string          | the event record serialised as a JSON string  |
| `hashed_json` | string          | SHA-256 hex digest of `data`                   |
| `loaded_at`   | timestamp (UTC) | the script's execution time                    |

Group by `batch_id` to collapse a run into one logical job; sort by `loaded_at`
to order jobs chronologically.

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
  --table analytics_db.events \
  --warehouse s3://my-bucket/warehouse
```

AWS credentials and region are read from the standard environment.

## CLI arguments

| argument                | required | default                  | description                                                              |
|-------------------------|----------|--------------------------|--------------------------------------------------------------------------|
| `-n`, `--total-rows`    | yes      | —                        | Total number of records to produce.                                      |
| `--table`               | yes      | —                        | Fully qualified `namespace.table` identifier (the namespace must exist). |
| `--warehouse`           | yes      | —                        | S3 warehouse URI for the Glue catalog (e.g. `s3://my-bucket/warehouse`). |
| `--catalog-name`        | no       | `glue`                   | Name of the Glue catalog to load.                                        |
| `--state-file`          | no       | `.batch_wap_state.json`  | Path to the JSON file that tracks the id sequence.                       |
| `--null-message`        | no       | `0`                      | Records receiving the null-message mutation (an integer or `ALL`).       |
| `--duplicate-id`        | no       | `0`                      | Records receiving the duplicate-id mutation (an integer or `ALL`).       |
| `--extra-user-id`       | no       | `0`                      | Records receiving the extra-user-id mutation (an integer or `ALL`).      |
| `--missing-event-ts`    | no       | `0`                      | Records receiving the missing-event-ts mutation (an integer or `ALL`).   |
| `--rename-event-type`   | no       | `0`                      | Records receiving the rename-event-type mutation (an integer or `ALL`).  |

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
  --table analytics_db.events --warehouse s3://my-bucket/warehouse
```

## Id uniqueness / state file

Record `id`s never repeat across runs. The last allocated id is stored in a
small JSON state file (`--state-file`, default `./.batch_wap_state.json`) and
advanced after each successful write. Delete the file to restart the sequence.
