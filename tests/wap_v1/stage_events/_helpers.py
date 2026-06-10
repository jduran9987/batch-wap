"""Shared test helpers and fakes for the WAP v1 test suite.

Imported by test modules as ``import _helpers`` (pytest adds the test
directory to ``sys.path``, matching the ``import _contract_api`` convention
in ``tests/batch_data_generator/``).

Provides:

- :class:`FakeReader` — returns a pre-built Polars frame; no DB required.
- :class:`FakeWriter` — captures calls from ``job.run``; never opens a
  ``clickhouse-connect`` client.
- :func:`make_source_frame` — builds a raw-events Polars frame from a list of
  ``data`` dicts, hashing each exactly as the source package does (SHA-256 of
  the UTF-8 encoded JSON bytes).

Contract-name aliases (mirrors ``_contract_api.py`` naming convention):

    NULL_FIELD      = "message"
    MISSING_FIELD   = "event_ts"
    RENAMED_FROM    = "event_type"
    RENAMED_TO      = "type"
    EXTRA_FIELD     = "user_id"
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import polars as pl

# ---------------------------------------------------------------------------
# Contract-name aliases (§findings doc §3)
# ---------------------------------------------------------------------------

NULL_FIELD: str = "message"
MISSING_FIELD: str = "event_ts"
RENAMED_FROM: str = "event_type"
RENAMED_TO: str = "type"
EXTRA_FIELD: str = "user_id"

# ---------------------------------------------------------------------------
# Source-frame helpers
# ---------------------------------------------------------------------------

_SOURCE_SCHEMA: dict[str, pl.DataType] = {
    "batch_id": pl.String,
    "data": pl.String,
    "hashed_json": pl.String,
    "loaded_at": pl.Datetime(time_unit="us", time_zone="UTC"),
}

_DEFAULT_LOADED_AT = datetime(2026, 6, 5, 11, 55, 0, tzinfo=timezone.utc)


def _sha256(data_str: str) -> str:
    """Return the SHA-256 hex digest of a UTF-8 string (mirrors source envelope)."""
    return hashlib.sha256(data_str.encode("utf-8")).hexdigest()


def make_source_frame(
    data_dicts: list[dict[str, Any]],
    *,
    batch_id: str = "batch-test",
    loaded_at: datetime = _DEFAULT_LOADED_AT,
) -> pl.DataFrame:
    """Build a raw-events Polars frame from a list of data dicts.

    Each dict is JSON-serialised and SHA-256 hashed exactly as the source
    package does (SHA-256 of the UTF-8 encoded JSON string), so dedup keys
    are realistic.

    Args:
        data_dicts: List of event record dicts (clean or mutated).
        batch_id: The ``batch_id`` value applied to every row.
        loaded_at: The ``loaded_at`` timestamp applied to every row.

    Returns:
        A Polars DataFrame with columns ``batch_id``, ``data``,
        ``hashed_json``, ``loaded_at`` matching the source table schema.
    """
    rows = []
    for d in data_dicts:
        data_str = json.dumps(d)
        rows.append(
            {
                "batch_id": batch_id,
                "data": data_str,
                "hashed_json": _sha256(data_str),
                "loaded_at": loaded_at,
            }
        )
    return pl.DataFrame(rows, schema=_SOURCE_SCHEMA)


# ---------------------------------------------------------------------------
# FakeReader
# ---------------------------------------------------------------------------


class FakeReader:
    """Test double for :class:`~batch_wap.ingestion.wap_v1.stage_events.clickhouse.Reader`.

    Returns the injected Polars frame from ``read_raw_events``; never opens
    a database connection.

    Args:
        df: The source frame to return (build with :func:`make_source_frame`).
    """

    def __init__(self, df: pl.DataFrame) -> None:
        """Store the pre-built frame.

        Args:
            df: The source frame returned from ``read_raw_events``.
        """
        self._df = df

    def read_raw_events(
        self,
        window_start: datetime,
        window_end: datetime,
    ) -> pl.DataFrame:
        """Return the injected frame regardless of the window bounds.

        Args:
            window_start: Ignored in the fake.
            window_end: Ignored in the fake.

        Returns:
            The frame supplied at construction time.
        """
        return self._df


# ---------------------------------------------------------------------------
# FakeWriter
# ---------------------------------------------------------------------------


class FakeWriter:
    """Test double for :class:`~batch_wap.ingestion.wap_v1.stage_events.clickhouse.Writer`.

    Captures every call from ``job.run``; constructs no ``clickhouse-connect``
    client.

    Attributes:
        ensure_tables_calls: Count of ``ensure_tables()`` invocations.
        staging_calls: List of ``(job_run_id, df)`` from
            ``replace_staging_partition``.
        quarantine_calls: List of ``(job_run_id, df)`` from
            ``replace_quarantine_partition``.
        stats_rows: List of dicts from ``write_stats``.
    """

    def __init__(self, *, raise_on: str | None = None) -> None:
        """Initialise the fake writer.

        Args:
            raise_on: When set, the named method raises ``RuntimeError`` when
                called.  Accepted values: ``"ensure_tables"``,
                ``"replace_staging_partition"``,
                ``"replace_quarantine_partition"``, ``"write_stats"``.
        """
        self.ensure_tables_calls: int = 0
        self.staging_calls: list[tuple[str, pl.DataFrame]] = []
        self.quarantine_calls: list[tuple[str, pl.DataFrame]] = []
        self.stats_rows: list[dict[str, Any]] = []
        self._raise_on = raise_on

    def ensure_tables(self) -> None:
        """Record the call; optionally raise."""
        self.ensure_tables_calls += 1
        if self._raise_on == "ensure_tables":
            raise RuntimeError("fake ensure_tables failure")

    def replace_staging_partition(
        self,
        job_run_id: str,
        df: pl.DataFrame,
    ) -> int:
        """Capture the call; optionally raise.

        Args:
            job_run_id: The partition key.
            df: The staging frame.

        Returns:
            Number of rows in ``df``.
        """
        if self._raise_on == "replace_staging_partition":
            raise RuntimeError("fake staging failure")
        self.staging_calls.append((job_run_id, df))
        return df.height

    def replace_quarantine_partition(
        self,
        job_run_id: str,
        df: pl.DataFrame,
    ) -> int:
        """Capture the call; optionally raise.

        Args:
            job_run_id: The partition key.
            df: The quarantine frame.

        Returns:
            Number of rows in ``df``.
        """
        if self._raise_on == "replace_quarantine_partition":
            raise RuntimeError("fake quarantine failure")
        self.quarantine_calls.append((job_run_id, df))
        return df.height

    def write_stats(self, row: dict[str, Any]) -> None:
        """Capture the stats row; optionally raise.

        Args:
            row: The stats dict from ``job.run``.
        """
        if self._raise_on == "write_stats":
            raise RuntimeError("fake write_stats failure")
        self.stats_rows.append(row)
