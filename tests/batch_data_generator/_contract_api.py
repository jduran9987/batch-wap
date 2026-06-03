"""Adapter layer for the ``batch_data_generator`` contract tests.

This is the only file that encodes assumptions about the package's public API
and its record-field names. The test modules import from here so that if a
future refactor renames things, only this file needs to change.

The tests consume the generator's output **in memory** and never touch the
sink, so they remain valid across storage-backend changes.
"""

from __future__ import annotations

from datetime import datetime, timezone

from batch_wap.sources.batch_data_generator.envelope import pack_rows
from batch_wap.sources.batch_data_generator.generator import (
    MutationCounts,
    Record,
    generate_records,
)
from batch_wap.sources.batch_data_generator.state import (
    read_last_id,
    write_last_id,
)

__all__ = [
    "EXTRA_FIELD",
    "MISSING_FIELD",
    "MutationCounts",
    "NULL_FIELD",
    "PK_FIELD",
    "RENAMED_FROM",
    "RENAMED_TO",
    "Record",
    "generate_records",
    "make_run_ts",
    "pack_rows",
    "read_last_id",
    "run",
    "write_last_id",
]

PK_FIELD = "id"
NULL_FIELD = "message"
RENAMED_FROM = "event_type"
RENAMED_TO = "type"
MISSING_FIELD = "event_ts"
EXTRA_FIELD = "user_id"


def make_run_ts() -> datetime:
    """Return a fixed UTC timestamp used by the tests.

    Returns:
        A timezone-aware UTC ``datetime`` suitable for ``event_ts``.
    """
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def run(total: int, start_id: int = 0, **mutations: int) -> list[Record]:
    """Generate ``total`` records with the given mutation counts.

    Args:
        total: Number of records to generate.
        start_id: Last id allocated by previous runs; new ids begin at
            ``start_id + 1``.
        **mutations: Per-mutation row counts (``null_message``,
            ``duplicate_id``, ``extra_user_id``, ``missing_event_ts``,
            ``rename_event_type``). Missing keys default to 0.

    Returns:
        The generated records, in order.
    """
    counts = MutationCounts(total=total, **mutations)
    return generate_records(counts, start_id, make_run_ts())
