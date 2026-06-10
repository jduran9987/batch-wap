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
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ("
        "    id               Int64,"
        "    event_type       LowCardinality(String),"
        "    event_ts         String,"
        "    message          String,"
        "    hashed_json      String,"
        "    batch_id         String,"
        "    source_loaded_at DateTime64(6, 'UTC'),"
        "    job_run_id       String,"
        "    processed_at     DateTime64(6, 'UTC') DEFAULT now64(6)"
        ") ENGINE = MergeTree"
        "PARTITION BY job_run_id"
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
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ("
        "    batch_id          String,"
        "    data              String,"
        "    hashed_json       String,"
        "    loaded_at         DateTime64(6, 'UTC'),"
        "    job_run_id        String,"
        "    validation_errors String,"
        "    error_count       UInt16,"
        "    quarantined_at    DateTime64(6, 'UTC') DEFAULT now64(6)"
        ") ENGINE = MergeTree"
        "PARTITION BY job_run_id"
        "ORDER BY (hashed_json)"
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
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ("
        "    job_run_id           String,"
        "    source_table         String DEFAULT 'raw.raw_events',"
        "    window_start         DateTime64(6, 'UTC'),"
        "    window_end           DateTime64(6, 'UTC'),"
        "    run_started_at       DateTime64(6, 'UTC'),"
        "    run_completed_at     DateTime64(6, 'UTC'),"
        "    latency_seconds      Float64,"
        "    rows_read            UInt64,"
        "    rows_written_staging UInt64,"
        "    rows_quarantined     UInt64,"
        "    unexpected_columns   Array(String),"
        "    status               LowCardinality(String),"
        "    error_message        Nullable(String),"
        "    created_at           DateTime64(6, 'UTC') DEFAULT now64(6)"
        ") ENGINE = ReplacingMergeTree(created_at)"
        "PARTITION BY toYYYYMM(window_start)"
        "ORDER BY (job_run_id)"
    )
