"""ClickHouse DDL and insert-column metadata for the three WAP v1 target tables.

This module is the single source of truth for table schemas.  Every
``CREATE TABLE IF NOT EXISTS`` statement and every ordered insert-column list is
defined here.  ``clickhouse.py`` imports from here; nothing else should hard-code
column names or DDL.

The three tables are:

- ``raw.stg_events``            — staging (one partition per window).
- ``quarantine.raw_events``     — failed rows (one partition per window; TTL).
- ``statistics.stg_events``     — one row per run (monthly partition;
                                   ``ReplacingMergeTree``).

``DEFAULT``-expression columns (``processed_at``, ``quarantined_at``,
``created_at``, ``source_table``) are excluded from the insert-column lists so
ClickHouse evaluates them at insert time.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# raw.stg_events — staging table
# ---------------------------------------------------------------------------

#: Columns supplied on INSERT (DEFAULT processed_at is excluded).
STG_EVENTS_INSERT_COLUMNS: list[str] = [
    "id",
    "event_type",
    "event_ts",
    "message",
    "hashed_json",
    "batch_id",
    "source_loaded_at",
    "job_run_id",
]


def stg_events_ddl(database: str = "raw", table: str = "stg_events") -> str:
    """Build the ``CREATE TABLE IF NOT EXISTS`` DDL for the staging table.

    Args:
        database: Target database (must already exist).
        table: Target table name.

    Returns:
        A ClickHouse ``CREATE TABLE IF NOT EXISTS`` statement.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
        "    id               Int64,\n"
        "    event_type       LowCardinality(String),\n"
        "    event_ts         String,\n"
        "    message          String,\n"
        "    hashed_json      String,\n"
        "    batch_id         String,\n"
        "    source_loaded_at DateTime64(6, 'UTC'),\n"
        "    job_run_id       String,\n"
        "    processed_at     DateTime64(6, 'UTC') DEFAULT now64(6)\n"
        ") ENGINE = MergeTree\n"
        "PARTITION BY job_run_id\n"
        "ORDER BY (id)"
    )


# ---------------------------------------------------------------------------
# quarantine.raw_events — quarantine table
# ---------------------------------------------------------------------------

#: Columns supplied on INSERT (DEFAULT quarantined_at is excluded).
QUARANTINE_EVENTS_INSERT_COLUMNS: list[str] = [
    "batch_id",
    "data",
    "hashed_json",
    "loaded_at",
    "job_run_id",
    "validation_errors",
    "error_count",
]


def quarantine_events_ddl(
    database: str = "quarantine",
    table: str = "raw_events",
) -> str:
    """Build the ``CREATE TABLE IF NOT EXISTS`` DDL for the quarantine table.

    Args:
        database: Target database (must already exist).
        table: Target table name.

    Returns:
        A ClickHouse ``CREATE TABLE IF NOT EXISTS`` statement.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
        "    batch_id          String,\n"
        "    data              String,\n"
        "    hashed_json       String,\n"
        "    loaded_at         DateTime64(6, 'UTC'),\n"
        "    job_run_id        String,\n"
        "    validation_errors String,\n"
        "    error_count       UInt16,\n"
        "    quarantined_at    DateTime64(6, 'UTC') DEFAULT now64(6)\n"
        ") ENGINE = MergeTree\n"
        "PARTITION BY job_run_id\n"
        "ORDER BY (hashed_json)\n"
        "TTL toDateTime(quarantined_at) + INTERVAL 14 DAY"
    )


# ---------------------------------------------------------------------------
# statistics.stg_events — statistics table
# ---------------------------------------------------------------------------

#: Columns supplied on INSERT (DEFAULT source_table and created_at excluded).
STATISTICS_STG_EVENTS_INSERT_COLUMNS: list[str] = [
    "job_run_id",
    "window_start",
    "window_end",
    "run_started_at",
    "run_completed_at",
    "latency_seconds",
    "rows_read",
    "rows_written_staging",
    "rows_quarantined",
    "unexpected_columns",
    "status",
    "error_message",
]


def statistics_stg_events_ddl(
    database: str = "statistics",
    table: str = "stg_events",
) -> str:
    """Build the ``CREATE TABLE IF NOT EXISTS`` DDL for the statistics table.

    Args:
        database: Target database (must already exist).
        table: Target table name.

    Returns:
        A ClickHouse ``CREATE TABLE IF NOT EXISTS`` statement.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
        "    job_run_id           String,\n"
        "    source_table         String DEFAULT 'raw.raw_events',\n"
        "    window_start         DateTime64(6, 'UTC'),\n"
        "    window_end           DateTime64(6, 'UTC'),\n"
        "    run_started_at       DateTime64(6, 'UTC'),\n"
        "    run_completed_at     DateTime64(6, 'UTC'),\n"
        "    latency_seconds      Float64,\n"
        "    rows_read            UInt64,\n"
        "    rows_written_staging UInt64,\n"
        "    rows_quarantined     UInt64,\n"
        "    unexpected_columns   Array(String),\n"
        "    status               LowCardinality(String),\n"
        "    error_message        Nullable(String),\n"
        "    created_at           DateTime64(6, 'UTC') DEFAULT now64(6)\n"
        ") ENGINE = ReplacingMergeTree(created_at)\n"
        "PARTITION BY toYYYYMM(window_start)\n"
        "ORDER BY (job_run_id)"
    )
