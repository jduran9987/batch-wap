"""WAP v1 staging-job orchestration.

This module ties together the pure helpers (``ids``, ``models``, ``flatten``)
and the I/O seam (``clickhouse.Reader`` / ``clickhouse.Writer``) into a single
``run`` function.  The function depends only on the protocols, so it is fully
unit-testable with :class:`~tests.wap_v1.conftest.FakeReader` /
:class:`~tests.wap_v1.conftest.FakeWriter` and never requires a live database.

Write order
-----------
For a window with derived id ``J``:

1. ``writer.ensure_tables()``
2. Read ``[window_start, window_end)`` from source.
3. Validate each ``data`` string; split into good / bad rows; collect
   unexpected-column names from valid rows.
4. Flatten good rows → staging frame; ``replace_staging_partition(J, ...)``.
5. Dedup bad rows on ``hashed_json``; ``replace_quarantine_partition(J, ...)``.
6. Insert one stats row (``write_stats``).

Failure handling
----------------
Any exception raised by steps 1–5 is caught.  A ``status='failed'`` stats row
is written (best-effort; a second exception from the stats write is caught and
logged so it cannot mask the original), then the original exception is
re-raised so the orchestrator's task fails and its retry fires.  Stage labels
are classified by :func:`_classify_stage` to a short label stored in
``error_message``.

Idempotency
-----------
``job_run_id`` is derived deterministically from ``window_start`` so any
retrigger computes the same id and replaces the same partitions.  A crash
between the staging and quarantine replaces leaves partial state; the retry
heals it by dropping and replacing both partitions unconditionally.  Stats is
``ReplacingMergeTree`` keyed on ``job_run_id``, so a successful retry's row
supersedes the earlier ``failed`` row.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import polars as pl

from batch_wap.ingestion.wap_v1.stage_events import flatten as flatten_mod
from batch_wap.ingestion.wap_v1.stage_events import models as models_mod
from batch_wap.ingestion.wap_v1.stage_events.clickhouse import Reader, Writer
from batch_wap.ingestion.wap_v1.stage_events.ids import derive_job_run_id

_log = logging.getLogger(__name__)

# Stage labels used in error_message on failure.
_STAGE_ENSURE = "ensure_tables_failed"
_STAGE_READ = "read_failed"
_STAGE_VALIDATE = "validation_failed"
_STAGE_STAGING = "staging_write_failed"
_STAGE_QUARANTINE = "quarantine_write_failed"
_STAGE_STATS = "stats_write_failed"
_STAGE_UNKNOWN = "unknown"


def _classify_stage(stage: str) -> str:
    """Return the stage label to store in ``error_message`` on failure.

    Args:
        stage: One of the ``_STAGE_*`` module-level constants.

    Returns:
        The same label (identity; the constants are already the canonical
        labels, but routing through a function keeps the caller readable and
        the mapping extensible).
    """
    return stage


def run(
    window_start: datetime,
    window_end: datetime,
    reader: Reader,
    writer: Writer,
) -> dict[str, Any]:
    """Execute one WAP v1 staging-job run for the given window.

    Args:
        window_start: Inclusive lower bound on ``loaded_at``; must be
            timezone-aware (enforced by :func:`ids.derive_job_run_id`).
        window_end: Exclusive upper bound on ``loaded_at``; must be
            timezone-aware.
        reader: Source reader (``ClickHouseReader`` in production; a
            ``FakeReader`` in tests).
        writer: Target writer (``ClickHouseWriter`` in production; a
            ``FakeWriter`` in tests).

    Returns:
        The stats row dict that was passed to ``writer.write_stats``.

    Raises:
        ValueError: If ``window_start`` is timezone-naive.
        Exception: Re-raises any exception from the read/validate/write
            pipeline after writing a ``status='failed'`` stats row.
    """
    job_run_id = derive_job_run_id(window_start)
    run_started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()

    stage = _STAGE_UNKNOWN
    try:
        stage = _STAGE_ENSURE
        writer.ensure_tables()

        stage = _STAGE_READ
        source_df = reader.read_raw_events(window_start, window_end)

        stage = _STAGE_VALIDATE
        good_rows, bad_rows, unexpected_columns = _validate_rows(
            source_df, job_run_id
        )

        stage = _STAGE_STAGING
        good_df = flatten_mod.flatten_valid(good_rows, job_run_id)
        writer.replace_staging_partition(job_run_id, good_df)

        stage = _STAGE_QUARANTINE
        bad_df = _build_quarantine_frame(bad_rows)
        bad_df = bad_df.unique(subset=["hashed_json"]) if bad_df.height else bad_df
        writer.replace_quarantine_partition(job_run_id, bad_df)

    except Exception:
        # Build a failed stats row and write it best-effort before re-raising.
        run_completed_at = datetime.now(timezone.utc)
        latency = time.monotonic() - t0
        failed_row = _build_stats_row(
            job_run_id=job_run_id,
            window_start=window_start,
            window_end=window_end,
            run_started_at=run_started_at,
            run_completed_at=run_completed_at,
            latency_seconds=latency,
            rows_read=0,
            rows_written_staging=0,
            rows_quarantined=0,
            unexpected_columns=[],
            status="failed",
            error_message=_classify_stage(stage),
        )
        try:
            writer.write_stats(failed_row)
        except Exception as stats_exc:  # noqa: BLE001
            _log.warning(
                "Failed to write failed-stats row for %s: %s", job_run_id, stats_exc
            )
        raise

    # Success path.
    run_completed_at = datetime.now(timezone.utc)
    latency = time.monotonic() - t0
    stats_row = _build_stats_row(
        job_run_id=job_run_id,
        window_start=window_start,
        window_end=window_end,
        run_started_at=run_started_at,
        run_completed_at=run_completed_at,
        latency_seconds=latency,
        rows_read=source_df.height,
        rows_written_staging=good_df.height,
        rows_quarantined=bad_df.height,
        unexpected_columns=unexpected_columns,
        status="success",
        error_message=None,
    )
    try:
        writer.write_stats(stats_row)
    except Exception:
        # Stats write failed after all data was committed — record a failed row
        # (best-effort) then re-raise so the orchestrator retries.
        stats_row["status"] = "failed"
        stats_row["error_message"] = _STAGE_STATS
        try:
            writer.write_stats(stats_row)
        except Exception as inner:  # noqa: BLE001
            _log.warning(
                "Failed to write failed-stats row for %s: %s", job_run_id, inner
            )
        raise
    return stats_row


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate_rows(
    source_df: pl.DataFrame,
    job_run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Validate every row in ``source_df`` and split into good / bad.

    Args:
        source_df: Raw-events frame from the reader.
        job_run_id: Attached to bad rows for the quarantine table.

    Returns:
        A three-tuple ``(good_rows, bad_rows, unexpected_columns)``:

        - ``good_rows``: Dicts with validated fields + source lineage, ready
          for :func:`flatten.flatten_valid`.
        - ``bad_rows``: Dicts with the original source envelope + structured
          error detail, ready for :func:`_build_quarantine_frame`.
        - ``unexpected_columns``: Sorted distinct extra-key names seen in
          **valid** rows only (used for stats).
    """
    good_rows: list[dict[str, Any]] = []
    bad_rows: list[dict[str, Any]] = []
    extra_keys_seen: set[str] = set()

    for row in source_df.iter_rows(named=True):
        result = models_mod.parse_and_validate(row["data"])
        if result.ok:
            extra_keys_seen.update(result.extra_keys)
            good_rows.append(
                {
                    **result.validated,
                    "hashed_json": row["hashed_json"],
                    "batch_id": row["batch_id"],
                    "loaded_at": row["loaded_at"],
                }
            )
        else:
            bad_rows.append(
                {
                    "batch_id": row["batch_id"],
                    "data": row["data"],
                    "hashed_json": row["hashed_json"],
                    "loaded_at": row["loaded_at"],
                    "job_run_id": job_run_id,
                    "validation_errors": json.dumps(result.errors),
                    "error_count": result.error_count,
                }
            )

    return good_rows, bad_rows, sorted(extra_keys_seen)


_QUARANTINE_SCHEMA: dict[str, pl.DataType] = {
    "batch_id": pl.String,
    "data": pl.String,
    "hashed_json": pl.String,
    "loaded_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "job_run_id": pl.String,
    "validation_errors": pl.String,
    "error_count": pl.UInt16,
}


def _build_quarantine_frame(bad_rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Convert bad-row dicts into a quarantine Polars DataFrame.

    Args:
        bad_rows: List of dicts produced by :func:`_validate_rows`.

    Returns:
        A Polars DataFrame with the quarantine insert columns and pinned
        dtypes.  Returns an empty frame with the correct schema when
        ``bad_rows`` is empty.
    """
    if not bad_rows:
        return pl.DataFrame(schema=_QUARANTINE_SCHEMA)
    return pl.DataFrame(bad_rows, schema=_QUARANTINE_SCHEMA)


def _build_stats_row(
    *,
    job_run_id: str,
    window_start: datetime,
    window_end: datetime,
    run_started_at: datetime,
    run_completed_at: datetime,
    latency_seconds: float,
    rows_read: int,
    rows_written_staging: int,
    rows_quarantined: int,
    unexpected_columns: list[str],
    status: str,
    error_message: str | None,
) -> dict[str, Any]:
    """Build the stats dict for ``writer.write_stats``.

    Args:
        job_run_id: Window partition key.
        window_start: Window lower bound.
        window_end: Window upper bound.
        run_started_at: Wall-clock start time.
        run_completed_at: Wall-clock end time.
        latency_seconds: Monotonic-clock duration.
        rows_read: Rows read from source.
        rows_written_staging: Rows written to staging.
        rows_quarantined: Rows written to quarantine.
        unexpected_columns: Distinct extra-key names from valid rows.
        status: ``'success'`` or ``'failed'``.
        error_message: Stage label on failure; ``None`` on success.

    Returns:
        A dict keyed by
        :data:`schemas.STATISTICS_STG_EVENTS_INSERT_COLUMNS`.
    """
    return {
        "job_run_id": job_run_id,
        "window_start": window_start,
        "window_end": window_end,
        "run_started_at": run_started_at,
        "run_completed_at": run_completed_at,
        "latency_seconds": latency_seconds,
        "rows_read": rows_read,
        "rows_written_staging": rows_written_staging,
        "rows_quarantined": rows_quarantined,
        "unexpected_columns": unexpected_columns,
        "status": status,
        "error_message": error_message,
    }
