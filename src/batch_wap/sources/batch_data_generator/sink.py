"""ClickHouse sink for the batch data generator.

Append-only writer that inserts envelope rows into a ClickHouse ``MergeTree``
table over the HTTP interface using ``clickhouse-connect``. The sink owns the
only code path that talks to the warehouse; the rest of the package stays
storage-agnostic.

Design contract
---------------
* **Append-only.** The sink never updates, deletes, or replaces existing rows.
* **Batched.** Rows are inserted in fixed-size batches (configurable) rather
  than one row at a time.
* **Duplicates tolerated on retry.** Transient insert errors are retried; if a
  retry results in a duplicate row, that is acceptable for this workload.
* **Creates the table, not the database.** If the target table is missing it is
  created with the schema below; the database must already exist.

Table schema (created on first write if absent)::

    CREATE TABLE IF NOT EXISTS <database>.<table> (
        batch_id    String,
        data        String,
        hashed_json String,
        loaded_at   DateTime64(6, 'UTC')
    ) ENGINE = MergeTree
    ORDER BY (loaded_at, batch_id)
    PARTITION BY toYYYYMM(loaded_at)
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

import clickhouse_connect

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

COLUMN_NAMES: list[str] = ["batch_id", "data", "hashed_json", "loaded_at"]
COLUMN_TYPES: list[str] = ["String", "String", "String", "DateTime64(6, 'UTC')"]


def _create_table_ddl(database: str, table: str) -> str:
    """Build the ``CREATE TABLE IF NOT EXISTS`` statement for the envelope table.

    Args:
        database: Target database (must already exist).
        table: Target table name.

    Returns:
        A ClickHouse SQL ``CREATE TABLE IF NOT EXISTS`` statement.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
        "    batch_id    String,\n"
        "    data        String,\n"
        "    hashed_json String,\n"
        "    loaded_at   DateTime64(6, 'UTC')\n"
        ") ENGINE = MergeTree\n"
        "ORDER BY (loaded_at, batch_id)\n"
        "PARTITION BY toYYYYMM(loaded_at)"
    )


def _chunked(rows: Iterable[tuple], size: int) -> Iterator[list[tuple]]:
    """Yield ``rows`` as fixed-size batches.

    Args:
        rows: An iterable of envelope row tuples.
        size: Maximum number of rows per emitted batch.

    Yields:
        Lists of at most ``size`` rows. The final batch may be smaller.
    """
    batch: list[tuple] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class ClickHouseSink:
    """Creates the envelope table if needed and appends rows in batches."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        table: str,
        batch_size: int,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
    ) -> None:
        """Initialise the sink and connect to ClickHouse.

        Args:
            host: ClickHouse hostname.
            port: ClickHouse HTTP port.
            username: ClickHouse username.
            password: ClickHouse password.
            database: Target database (must already exist).
            table: Target table name; created on first write if missing.
            batch_size: Maximum rows per insert call.
            max_retries: Number of additional attempts per batch on failure.
            retry_backoff_seconds: Base seconds for exponential backoff.

        Raises:
            ValueError: If ``batch_size`` is not positive.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._database = database
        self._table = table
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._client: Client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )

    def _ensure_table(self) -> None:
        """Create the target table if it does not already exist."""
        self._client.command(_create_table_ddl(self._database, self._table))

    def _insert_with_retry(self, batch: list[tuple]) -> None:
        """Insert one batch, retrying transient failures.

        Args:
            batch: The rows to insert.

        Raises:
            Exception: Re-raises the last error if all attempts fail.
        """
        attempts = self._max_retries + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                self._client.insert(
                    table=self._table,
                    data=batch,
                    column_names=COLUMN_NAMES,
                    column_type_names=COLUMN_TYPES,
                    database=self._database,
                )
                return
            except Exception as error:  # noqa: BLE001 - re-raised below
                last_error = error
                if attempt == attempts - 1:
                    break
                time.sleep(self._retry_backoff_seconds * (2**attempt))
        assert last_error is not None
        raise last_error

    def write(self, rows: Iterable[tuple]) -> int:
        """Create the table if absent, then insert ``rows`` in batches.

        Args:
            rows: An iterable of envelope row tuples in the order defined by
                :data:`COLUMN_NAMES`.

        Returns:
            The total number of rows inserted.
        """
        self._ensure_table()
        total = 0
        for batch in _chunked(rows, self._batch_size):
            self._insert_with_retry(batch)
            total += len(batch)
        return total

    def close(self) -> None:
        """Close the underlying ClickHouse client connection."""
        self._client.close()
