"""Spec test #2 — staging routing.

Clean rows and extra_user_id rows must reach replace_staging_partition.
The extra_user_id row is valid; user_id must appear in unexpected_columns in
the stats row.  No extra_user_id or clean row should reach quarantine.

Optional (in-scope): duplicate_id rows also reach staging — documents the
deliberate non-quarantine of duplicate ids.

No DB client is constructed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import _helpers

from batch_wap.ingestion.wap_v1.stage_events import job

EXTRA_FIELD = _helpers.EXTRA_FIELD
FakeReader = _helpers.FakeReader
FakeWriter = _helpers.FakeWriter
make_source_frame = _helpers.make_source_frame

_UTC = timezone.utc
_WINDOW_START = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)
_WINDOW_END = datetime(2026, 6, 5, 12, 0, 0, tzinfo=_UTC)

_CLEAN = {
    "id": 1,
    "event_type": "event_one",
    "event_ts": "2026-06-05T11:50:00+00:00",
    "message": "hello",
}


def _run(data_dicts: list[dict]) -> FakeWriter:
    """Run the job and return the writer."""
    df = make_source_frame(data_dicts)
    reader = FakeReader(df)
    writer = FakeWriter()
    job.run(_WINDOW_START, _WINDOW_END, reader, writer)
    return writer


class TestCleanRowsReachStaging:
    """Clean rows are routed to staging."""

    def test_staging_row_count(self) -> None:
        """One clean row → one staging row."""
        writer = _run([_CLEAN])
        _, sdf = writer.staging_calls[0]
        assert sdf.height == 1

    def test_no_quarantine_rows(self) -> None:
        """Clean row generates no quarantine rows."""
        writer = _run([_CLEAN])
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 0

    def test_multiple_clean_rows(self) -> None:
        """Multiple clean rows all reach staging."""
        rows = [{**_CLEAN, "id": i} for i in range(1, 6)]
        writer = _run(rows)
        _, sdf = writer.staging_calls[0]
        assert sdf.height == 5


class TestExtraUserIdReachesStaging:
    """extra_user_id rows are valid and must reach staging."""

    def test_reaches_staging(self) -> None:
        """Extra user_id row is routed to staging."""
        data = {**_CLEAN, EXTRA_FIELD: 999}
        writer = _run([data])
        _, sdf = writer.staging_calls[0]
        assert sdf.height == 1

    def test_does_not_reach_quarantine(self) -> None:
        """Extra user_id row does not reach quarantine."""
        data = {**_CLEAN, EXTRA_FIELD: 999}
        writer = _run([data])
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 0

    def test_user_id_in_unexpected_columns(self) -> None:
        """user_id appears in stats unexpected_columns."""
        data = {**_CLEAN, EXTRA_FIELD: 999}
        writer = _run([data])
        stats = writer.stats_rows[0]
        assert EXTRA_FIELD in stats["unexpected_columns"]

    def test_user_id_not_in_staging_columns(self) -> None:
        """user_id must NOT be written to the staging frame."""
        data = {**_CLEAN, EXTRA_FIELD: 999}
        writer = _run([data])
        _, sdf = writer.staging_calls[0]
        assert EXTRA_FIELD not in sdf.columns


class TestDuplicateIdReachesStaging:
    """Duplicate-id rows reach staging; dedup is downstream's responsibility."""

    def test_reaches_staging(self) -> None:
        """A row with a duplicate id passes validation and reaches staging."""
        row0 = {**_CLEAN, "id": 1}
        row_dup = {**_CLEAN, "id": 1, "message": "duplicate id row"}
        writer = _run([row0, row_dup])
        _, sdf = writer.staging_calls[0]
        # Both rows pass — duplicate id is not a structural failure
        assert sdf.height == 2

    def test_does_not_reach_quarantine(self) -> None:
        """Duplicate-id rows are not quarantined."""
        row0 = {**_CLEAN, "id": 1}
        row_dup = {**_CLEAN, "id": 1, "message": "duplicate id row"}
        writer = _run([row0, row_dup])
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 0


class TestStatsCountsOnSuccessPath:
    """Stats row reflects correct counts."""

    def test_rows_read(self) -> None:
        """rows_read equals the total source rows."""
        rows = [{**_CLEAN, "id": i} for i in range(1, 4)]
        writer = _run(rows)
        assert writer.stats_rows[0]["rows_read"] == 3

    def test_rows_written_staging(self) -> None:
        """rows_written_staging equals the number of valid rows."""
        rows = [{**_CLEAN, "id": i} for i in range(1, 4)]
        writer = _run(rows)
        assert writer.stats_rows[0]["rows_written_staging"] == 3

    def test_rows_quarantined_zero(self) -> None:
        """rows_quarantined is zero for a clean run."""
        writer = _run([_CLEAN])
        assert writer.stats_rows[0]["rows_quarantined"] == 0

    def test_status_success(self) -> None:
        """Status is 'success' on a clean run."""
        writer = _run([_CLEAN])
        assert writer.stats_rows[0]["status"] == "success"

    def test_error_message_none_on_success(self) -> None:
        """error_message is None on success."""
        writer = _run([_CLEAN])
        assert writer.stats_rows[0]["error_message"] is None

    def test_unexpected_columns_empty_for_clean_run(self) -> None:
        """unexpected_columns is empty when no extra keys are seen."""
        writer = _run([_CLEAN])
        assert writer.stats_rows[0]["unexpected_columns"] == []

    def test_unexpected_columns_sorted_distinct(self) -> None:
        """Multiple rows with the same extra key → one entry in unexpected_columns."""
        rows = [{**_CLEAN, "id": i, EXTRA_FIELD: i * 10} for i in range(1, 4)]
        writer = _run(rows)
        uc = writer.stats_rows[0]["unexpected_columns"]
        assert uc == sorted(set(uc))
        assert uc.count(EXTRA_FIELD) == 1
