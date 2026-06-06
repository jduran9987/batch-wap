"""Flatten validated event records into a staging Polars DataFrame.

This module is a pure transformation: it accepts already-validated records
(produced by :mod:`models`) together with their source lineage, and emits a
Polars DataFrame whose columns match exactly the insert columns of
``raw.stg_events``.  Extra keys are dropped here; they are captured for stats
by the caller, not written to staging.

The ``processed_at`` column is intentionally absent from the output frame —
ClickHouse applies its ``DEFAULT now64(6)`` expression on insert.
"""

from __future__ import annotations

from typing import Any

import polars as pl

# Ordered list of columns written to raw.stg_events (excluding DEFAULT columns).
# Must match STG_EVENTS_INSERT_COLUMNS in schemas.py — kept in sync manually;
# schemas.py is the authoritative source for the DDL.
_STAGING_COLUMNS: list[str] = [
    "id",
    "event_type",
    "event_ts",
    "message",
    "hashed_json",
    "batch_id",
    "source_loaded_at",
    "job_run_id",
]

_STAGING_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "event_type": pl.String,
    "event_ts": pl.String,
    "message": pl.String,
    "hashed_json": pl.String,
    "batch_id": pl.String,
    "source_loaded_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "job_run_id": pl.String,
}


def flatten_valid(
    good_rows: list[dict[str, Any]],
    job_run_id: str,
) -> pl.DataFrame:
    """Convert validated event records into a staging DataFrame.

    Each element of ``good_rows`` must contain:

    - Validated event fields: ``id`` (int), ``event_type`` (str),
      ``event_ts`` (str), ``message`` (str).
    - Source lineage: ``batch_id`` (str), ``loaded_at`` (datetime),
      ``hashed_json`` (str).

    Extra keys in the dict are silently dropped.

    Args:
        good_rows: List of dicts, one per validated event record.  May be empty.
        job_run_id: The window's partition key, attached to every row.

    Returns:
        A Polars DataFrame with exactly the staging insert columns and pinned
        dtypes (``id`` as Int64, timestamps as ``Datetime(us, UTC)``).
        Returns an empty frame with the correct schema when ``good_rows`` is
        empty.
    """
    if not good_rows:
        return pl.DataFrame(schema=_STAGING_SCHEMA)

    records = [
        {
            "id": row["id"],
            "event_type": row["event_type"],
            "event_ts": row["event_ts"],
            "message": row["message"],
            "hashed_json": row["hashed_json"],
            "batch_id": row["batch_id"],
            "source_loaded_at": row["loaded_at"],
            "job_run_id": job_run_id,
        }
        for row in good_rows
    ]
    return pl.DataFrame(records, schema=_STAGING_SCHEMA)
