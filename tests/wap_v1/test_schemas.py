"""Light tests asserting the DDL shape from schemas.py.

Mirrors the _create_table_ddl assertion pattern in test_sink.py.  The key
invariants are: staging and quarantine use PARTITION BY job_run_id; stats uses
ReplacingMergeTree and monthly partitioning.
"""

from __future__ import annotations

from batch_wap.ingestion.wap_v1.schemas import (
    QUARANTINE_EVENTS_INSERT_COLUMNS,
    STATISTICS_STG_EVENTS_INSERT_COLUMNS,
    STG_EVENTS_INSERT_COLUMNS,
    quarantine_events_ddl,
    statistics_stg_events_ddl,
    stg_events_ddl,
)


class TestStagingDDL:
    """raw.stg_events DDL invariants."""

    def test_contains_partition_by_job_run_id(self) -> None:
        """DDL contains PARTITION BY job_run_id."""
        ddl = stg_events_ddl()
        assert "PARTITION BY job_run_id" in ddl

    def test_contains_mergetree(self) -> None:
        """DDL specifies a MergeTree engine."""
        ddl = stg_events_ddl()
        assert "MergeTree" in ddl

    def test_contains_id_int64(self) -> None:
        """DDL declares id as Int64."""
        ddl = stg_events_ddl()
        assert "Int64" in ddl

    def test_contains_processed_at_default(self) -> None:
        """DDL has a DEFAULT expression for processed_at."""
        ddl = stg_events_ddl()
        assert "processed_at" in ddl
        assert "DEFAULT" in ddl

    def test_custom_database_and_table(self) -> None:
        """Custom database and table names are quoted correctly."""
        ddl = stg_events_ddl(database="mydb", table="mytbl")
        assert "`mydb`.`mytbl`" in ddl

    def test_insert_columns_does_not_include_processed_at(self) -> None:
        """processed_at is excluded from insert columns (it has a DEFAULT)."""
        assert "processed_at" not in STG_EVENTS_INSERT_COLUMNS

    def test_insert_columns_includes_required_fields(self) -> None:
        """All required staging fields are present in the insert-column list."""
        required = (
            "id",
            "event_type",
            "event_ts",
            "message",
            "hashed_json",
            "batch_id",
            "source_loaded_at",
            "job_run_id",
        )
        for col in required:
            assert col in STG_EVENTS_INSERT_COLUMNS


class TestQuarantineDDL:
    """quarantine.raw_events DDL invariants."""

    def test_contains_partition_by_job_run_id(self) -> None:
        """DDL contains PARTITION BY job_run_id."""
        ddl = quarantine_events_ddl()
        assert "PARTITION BY job_run_id" in ddl

    def test_contains_ttl(self) -> None:
        """DDL declares a TTL clause for retention."""
        ddl = quarantine_events_ddl()
        assert "TTL" in ddl

    def test_contains_14_day(self) -> None:
        """Default retention is 14 days."""
        ddl = quarantine_events_ddl()
        assert "14 DAY" in ddl

    def test_contains_quarantined_at_default(self) -> None:
        """DDL has a DEFAULT expression for quarantined_at."""
        ddl = quarantine_events_ddl()
        assert "quarantined_at" in ddl
        assert "DEFAULT" in ddl

    def test_insert_columns_does_not_include_quarantined_at(self) -> None:
        """quarantined_at is excluded from insert columns (it has a DEFAULT)."""
        assert "quarantined_at" not in QUARANTINE_EVENTS_INSERT_COLUMNS

    def test_insert_columns_includes_envelope_fields(self) -> None:
        """Source envelope fields are present in the insert-column list."""
        for col in ("batch_id", "data", "hashed_json", "loaded_at", "job_run_id"):
            assert col in QUARANTINE_EVENTS_INSERT_COLUMNS


class TestStatsDDL:
    """statistics.stg_events DDL invariants."""

    def test_contains_replacing_mergetree(self) -> None:
        """DDL specifies ReplacingMergeTree for rerun dedup."""
        ddl = statistics_stg_events_ddl()
        assert "ReplacingMergeTree" in ddl

    def test_contains_monthly_partition(self) -> None:
        """DDL partitions monthly by window_start."""
        ddl = statistics_stg_events_ddl()
        assert "PARTITION BY toYYYYMM(window_start)" in ddl

    def test_order_by_job_run_id(self) -> None:
        """DDL orders by job_run_id (the ReplacingMergeTree dedup key)."""
        ddl = statistics_stg_events_ddl()
        assert "ORDER BY (job_run_id)" in ddl

    def test_contains_created_at_default(self) -> None:
        """DDL has a DEFAULT for created_at (the ReplacingMergeTree version column)."""
        ddl = statistics_stg_events_ddl()
        assert "created_at" in ddl
        assert "DEFAULT" in ddl

    def test_insert_columns_does_not_include_created_at(self) -> None:
        """created_at is excluded from insert columns (it has a DEFAULT)."""
        assert "created_at" not in STATISTICS_STG_EVENTS_INSERT_COLUMNS

    def test_insert_columns_does_not_include_source_table(self) -> None:
        """source_table is excluded from insert columns (it has a DEFAULT)."""
        assert "source_table" not in STATISTICS_STG_EVENTS_INSERT_COLUMNS

    def test_insert_columns_includes_key_stats_fields(self) -> None:
        """Key stats fields are present in the insert-column list."""
        for col in ("job_run_id", "window_start", "window_end", "rows_read", "status"):
            assert col in STATISTICS_STG_EVENTS_INSERT_COLUMNS
