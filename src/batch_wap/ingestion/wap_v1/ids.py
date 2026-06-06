"""Job-run identifier derivation for the WAP v1 staging job.

The ``job_run_id`` is the single most important property in the idempotency
design.  Because it is a **pure function** of ``window_start``, any retrigger
of the same window computes the same id, lands on the same partition, and
therefore replaces its own prior output rather than duplicating it.
"""

from __future__ import annotations

from datetime import datetime, timezone


def derive_job_run_id(window_start: datetime) -> str:
    """Return the deterministic partition key for a 10-minute processing window.

    The id is ``"w"`` followed by ``window_start`` formatted as ``%Y%m%d%H%M``
    in UTC, e.g. ``"w202606051150"`` for the window starting at 11:50 UTC on
    2026-06-05.

    Args:
        window_start: The inclusive lower bound of the processing window.  Must
            be timezone-aware; a naive datetime is rejected to prevent silent
            local-timezone drift that would place the window on the wrong
            partition.

    Returns:
        A string of the form ``"w" + <14-char UTC timestamp>``.

    Raises:
        ValueError: If ``window_start`` has no timezone information (``tzinfo``
            is ``None``).
    """
    if window_start.tzinfo is None:
        raise ValueError(
            "window_start must be timezone-aware; "
            "naive datetimes are rejected to prevent local-tz partition drift. "
            f"Got: {window_start!r}"
        )
    return "w" + window_start.astimezone(timezone.utc).strftime("%Y%m%d%H%M")
