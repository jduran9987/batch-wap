"""ClickHouse I/O for the WAP v1 staging job.

This is the **only** module in the package that imports ``clickhouse_connect``.
All other modules are pure or accept the :class:`Reader` / :class:`Writer`
protocols injected by the caller, making unit tests possible without a live
database.

Protocols
---------
:class:`Reader` and :class:`Writer` define the minimal interface that
``job.run`` depends on.  The concrete implementations
(:class:`ClickHouseReader`, :class:`ClickHouseWriter`) satisfy these protocols
and are exercised by integration tests only.

Partition-replace write path
-----------------------------
Both ``replace_staging_partition`` and ``replace_quarantine_partition``
implement **unconditional drop, conditional insert**::

    ALTER TABLE <tbl> DROP PARTITION '{job_run_id}'   # always
    if df.height:
        client.insert(...)

The drop is unconditional because a rerun that produces *zero* rows for a
table must still clear any stale rows left by a prior attempt.  A
``DROP PARTITION`` on a non-existent partition is a ClickHouse no-op (not an
error), so the first run and every retry are safe with no ``IF EXISTS`` guard.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

import polars as pl

from batch_wap.ingestion.wap_v1.stage_events.schemas import (
    QUARANTINE_EVENTS_INSERT_COLUMNS,
    STATISTICS_STG_EVENTS_INSERT_COLUMNS,
    STG_EVENTS_INSERT_COLUMNS,
    quarantine_events_ddl,
    statistics_stg_events_ddl,
    stg_events_ddl,
)

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client


# ---------------------------------------------------------------------------
# Protocols — the test seam
# ---------------------------------------------------------------------------


class Reader(Protocol):
    """Protocol for reading a windowed slice of raw events."""

    def read_raw_events(
        self,
        window_start: datetime,
        window_end: datetime,
    ) -> pl.DataFrame:
        """Return source rows whose ``loaded_at`` is in ``[window_start, window_end)``.

        Args:
            window_start: Inclusive lower bound on ``loaded_at``.
            window_end: Exclusive upper bound on ``loaded_at``.

        Returns:
            A Polars DataFrame with columns ``batch_id``, ``data``,
            ``hashed_json``, ``loaded_at``.
        """
        ...


class Writer(Protocol):
    """Protocol for writing the three WAP v1 target tables."""

    def ensure_tables(self) -> None:
        """Create the three target tables if they do not already exist.

        Raises:
            RuntimeError: If a target database is absent.
        """
        ...

    def replace_staging_partition(
        self,
        job_run_id: str,
        df: pl.DataFrame,
    ) -> int:
        """Drop and replace the staging partition for ``job_run_id``.

        Args:
            job_run_id: The partition key for this window.
            df: The flattened staging rows.

        Returns:
            The number of rows inserted (0 if ``df`` is empty).
        """
        ...

    def replace_quarantine_partition(
        self,
        job_run_id: str,
        df: pl.DataFrame,
    ) -> int:
        """Drop and replace the quarantine partition for ``job_run_id``.

        Args:
            job_run_id: The partition key for this window.
            df: The deduped quarantine rows.

        Returns:
            The number of rows inserted (0 if ``df`` is empty).
        """
        ...

    def write_stats(self, row: dict[str, Any]) -> None:
        """Insert one statistics row.

        ``ReplacingMergeTree`` handles reruns; no drop is needed.

        Args:
            row: A dict keyed by :data:`schemas.STATISTICS_STG_EVENTS_INSERT_COLUMNS`.
        """
        ...


# ---------------------------------------------------------------------------
# Concrete implementations — integration-tested only
# ---------------------------------------------------------------------------


class ClickHouseReader:
    """Reads a windowed slice of ``raw.raw_events`` via the HTTP interface.

    Args:
        client: An active ``clickhouse-connect`` client.
        database: Source database name.
        table: Source table name.
    """

    def __init__(
        self,
        client: Client,
        database: str = "raw",
        table: str = "raw_events",
    ) -> None:
        """Initialise with a live ClickHouse client.

        Args:
            client: An active ``clickhouse-connect`` HTTP client.
            database: Database that contains the source table.
            table: Source table name.
        """
        self._client = client
        self._database = database
        self._table = table

    def read_raw_events(
        self,
        window_start: datetime,
        window_end: datetime,
    ) -> pl.DataFrame:
        """Return source rows in the half-open interval ``[window_start, window_end)``.

        Uses a parameterised query so timestamps are bound safely and the
        range scan is efficient (the source table is ordered by
        ``(loaded_at, batch_id)``).

        Args:
            window_start: Inclusive lower bound on ``loaded_at``.
            window_end: Exclusive upper bound on ``loaded_at``.

        Returns:
            A Polars DataFrame with columns ``batch_id``, ``data``,
            ``hashed_json``, ``loaded_at``.
        """
        query = (
            f"SELECT batch_id, data, hashed_json, loaded_at"
            f" FROM `{self._database}`.`{self._table}`"
            f" WHERE loaded_at >= {{start:DateTime64(6, 'UTC')}}"
            f"   AND loaded_at <  {{end:DateTime64(6, 'UTC')}}"
        )
        result = self._client.query_arrow(
            query,
            parameters={"start": window_start, "end": window_end},
        )
        return pl.from_arrow(result)


class ClickHouseWriter:
    """Writes the three WAP v1 target tables via the HTTP interface.

    Owns all DDL and DML for the staging job.  The only module in the package
    that constructs or calls a ``clickhouse-connect`` client.

    Args:
        client: An active ``clickhouse-connect`` client.
        stg_database: Database for ``stg_events``.
        quarantine_database: Database for the quarantine table.
        statistics_database: Database for the statistics table.
    """

    def __init__(
        self,
        client: Client,
        stg_database: str = "raw",
        quarantine_database: str = "quarantine",
        statistics_database: str = "statistics",
    ) -> None:
        """Initialise with a live ClickHouse client and database names.

        Args:
            client: An active ``clickhouse-connect`` HTTP client.
            stg_database: Database for ``raw.stg_events``.
            quarantine_database: Database for ``quarantine.raw_events``.
            statistics_database: Database for ``statistics.stg_events``.
        """
        self._client = client
        self._stg_db = stg_database
        self._quarantine_db = quarantine_database
        self._statistics_db = statistics_database

    def ensure_tables(self) -> None:
        """Create the three target tables if they do not already exist.

        Runs three ``CREATE TABLE IF NOT EXISTS`` statements using the DDL from
        :mod:`schemas`.  Databases must already exist; a missing database
        surfaces as a ClickHouse error wrapped with a descriptive message.

        Raises:
            RuntimeError: If a target database is absent (wraps the underlying
                ClickHouse error with the database name).
        """
        targets = [
            (stg_events_ddl(self._stg_db, "stg_events"), self._stg_db),
            (
                quarantine_events_ddl(self._quarantine_db, "raw_events"),
                self._quarantine_db,
            ),
            (
                statistics_stg_events_ddl(self._statistics_db, "stg_events"),
                self._statistics_db,
            ),
        ]
        for ddl, db in targets:
            try:
                self._client.command(ddl)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to ensure table in database '{db}' — "
                    f"confirm the database exists. Original error: {exc}"
                ) from exc

    def replace_staging_partition(
        self,
        job_run_id: str,
        df: pl.DataFrame,
    ) -> int:
        """Atomically replace the staging partition for ``job_run_id``.

        The partition is **always** dropped first (even when ``df`` is empty)
        so stale rows from a prior attempt are removed.  The insert is skipped
        only when ``df`` is empty.

        Args:
            job_run_id: The partition key (e.g. ``"w202606051150"``).
            df: Flattened staging rows; may be empty.

        Returns:
            Number of rows inserted.
        """
        return self._replace_partition(
            database=self._stg_db,
            table="stg_events",
            job_run_id=job_run_id,
            df=df,
            column_names=STG_EVENTS_INSERT_COLUMNS,
        )

    def replace_quarantine_partition(
        self,
        job_run_id: str,
        df: pl.DataFrame,
    ) -> int:
        """Atomically replace the quarantine partition for ``job_run_id``.

        The partition is **always** dropped first so stale rows from a prior
        attempt (including partial crash state) are removed before inserting
        the recomputed quarantine rows.

        Args:
            job_run_id: The partition key.
            df: Deduped quarantine rows; may be empty.

        Returns:
            Number of rows inserted.
        """
        return self._replace_partition(
            database=self._quarantine_db,
            table="raw_events",
            job_run_id=job_run_id,
            df=df,
            column_names=QUARANTINE_EVENTS_INSERT_COLUMNS,
        )

    def write_stats(self, row: dict[str, Any]) -> None:
        """Insert one statistics row into ``statistics.stg_events``.

        ``ReplacingMergeTree`` keyed on ``job_run_id`` handles reruns; no
        partition drop is needed for a single-row-per-run table.

        Args:
            row: A dict keyed by
                :data:`schemas.STATISTICS_STG_EVENTS_INSERT_COLUMNS`.
        """
        data = [[row[col] for col in STATISTICS_STG_EVENTS_INSERT_COLUMNS]]
        self._client.insert(
            table="stg_events",
            data=data,
            column_names=STATISTICS_STG_EVENTS_INSERT_COLUMNS,
            database=self._statistics_db,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _replace_partition(
        self,
        database: str,
        table: str,
        job_run_id: str,
        df: pl.DataFrame,
        column_names: list[str],
    ) -> int:
        """Drop a partition and optionally insert replacement rows.

        Args:
            database: Target database.
            table: Target table.
            job_run_id: Partition key to drop and refill.
            df: Rows to insert; skipped when empty.
            column_names: Ordered column names for the INSERT.

        Returns:
            Number of rows inserted (0 if ``df`` is empty).
        """
        # Always drop — clears stale/partial state from prior attempts.
        # DROP on a non-existent partition is a no-op in ClickHouse.
        self._client.command(
            f"ALTER TABLE `{database}`.`{table}` DROP PARTITION %(job_run_id)s",
            parameters={"job_run_id": job_run_id},
        )
        if not df.height:
            return 0
        self._client.insert(
            table=table,
            data=df.rows(),
            column_names=column_names,
            database=database,
        )
        return df.height
