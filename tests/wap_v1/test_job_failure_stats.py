"""Failure-path test — job.run writes a failed stats row and re-raises.

When a stage in the pipeline raises, job.run must:
  1. Call write_stats exactly once with status='failed' and
     error_message matching the stage label.
  2. Re-raise the original exception so the orchestrator sees a failure.

No DB client is constructed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import _helpers
import pytest

from batch_wap.ingestion.wap_v1 import job

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


def _run_with_failure(raise_on: str) -> tuple[FakeWriter, Exception]:
    """Run the job with a writer that raises on the specified method.

    Returns the writer and the caught exception.
    """
    df = make_source_frame([_CLEAN])
    reader = FakeReader(df)
    writer = FakeWriter(raise_on=raise_on)
    exc: Exception | None = None
    try:
        job.run(_WINDOW_START, _WINDOW_END, reader, writer)
    except Exception as e:  # noqa: BLE001
        exc = e
    assert exc is not None, "Expected job.run to raise"
    return writer, exc


class TestStagingFailureWritesFailedStats:
    """replace_staging_partition raises → failed stats row + re-raise."""

    def test_stats_written_once(self) -> None:
        """Exactly one stats row is written even when staging fails."""
        writer, _ = _run_with_failure("replace_staging_partition")
        assert len(writer.stats_rows) == 1

    def test_status_is_failed(self) -> None:
        """Stats row status is 'failed'."""
        writer, _ = _run_with_failure("replace_staging_partition")
        assert writer.stats_rows[0]["status"] == "failed"

    def test_error_message_is_staging_write_failed(self) -> None:
        """error_message is the staging stage label."""
        writer, _ = _run_with_failure("replace_staging_partition")
        assert writer.stats_rows[0]["error_message"] == "staging_write_failed"

    def test_original_exception_re_raised(self) -> None:
        """The original RuntimeError from the fake writer is re-raised."""
        _, exc = _run_with_failure("replace_staging_partition")
        assert isinstance(exc, RuntimeError)
        assert "fake staging failure" in str(exc)

    def test_rows_written_staging_is_zero_on_failure(self) -> None:
        """rows_written_staging is 0 when staging fails."""
        writer, _ = _run_with_failure("replace_staging_partition")
        assert writer.stats_rows[0]["rows_written_staging"] == 0

    def test_rows_quarantined_is_zero_on_failure(self) -> None:
        """rows_quarantined is 0 on failure (unverified counts not reported)."""
        writer, _ = _run_with_failure("replace_staging_partition")
        assert writer.stats_rows[0]["rows_quarantined"] == 0


class TestQuarantineFailureWritesFailedStats:
    """replace_quarantine_partition raises → failed stats with quarantine label."""

    def test_status_is_failed(self) -> None:
        """Stats status is 'failed' when quarantine write fails."""
        writer, _ = _run_with_failure("replace_quarantine_partition")
        assert writer.stats_rows[0]["status"] == "failed"

    def test_error_message_is_quarantine_write_failed(self) -> None:
        """error_message is the quarantine stage label."""
        writer, _ = _run_with_failure("replace_quarantine_partition")
        assert writer.stats_rows[0]["error_message"] == "quarantine_write_failed"

    def test_original_exception_re_raised(self) -> None:
        """The original RuntimeError is re-raised after writing failed stats."""
        _, exc = _run_with_failure("replace_quarantine_partition")
        assert isinstance(exc, RuntimeError)


class TestEnsureTablesFailureWritesFailedStats:
    """ensure_tables raises → failed stats with ensure_tables label."""

    def test_status_is_failed(self) -> None:
        """Stats status is 'failed' when ensure_tables fails."""
        writer, _ = _run_with_failure("ensure_tables")
        assert writer.stats_rows[0]["status"] == "failed"

    def test_error_message_is_ensure_tables_failed(self) -> None:
        """error_message is the ensure_tables stage label."""
        writer, _ = _run_with_failure("ensure_tables")
        assert writer.stats_rows[0]["error_message"] == "ensure_tables_failed"


class TestReadFailureWritesFailedStats:
    """Reader raises → failed stats with read_failed label."""

    def test_status_is_failed(self) -> None:
        """Stats status is 'failed' when the reader raises."""

        class FailingReader:
            def read_raw_events(self, window_start, window_end):  # noqa: ANN001
                """Raise unconditionally."""
                raise RuntimeError("read error")

        writer = FakeWriter()
        exc: Exception | None = None
        try:
            job.run(_WINDOW_START, _WINDOW_END, FailingReader(), writer)
        except Exception as e:  # noqa: BLE001
            exc = e
        assert exc is not None
        assert writer.stats_rows[0]["status"] == "failed"
        assert writer.stats_rows[0]["error_message"] == "read_failed"


class TestStatsWriteFailureDoesNotMaskOriginal:
    """If write_stats itself fails, the original exception is still re-raised."""

    def test_original_exception_propagates_when_stats_also_fails(self) -> None:
        """Both staging and stats fail; the staging RuntimeError must propagate."""

        class DoubleFailWriter(FakeWriter):
            """Fails on staging AND on write_stats."""

            def replace_staging_partition(self, job_run_id, df):  # noqa: ANN001
                """Raise unconditionally."""
                raise RuntimeError("staging error")

            def write_stats(self, row):  # noqa: ANN001
                """Raise unconditionally."""
                raise RuntimeError("stats write also failed")

        df = make_source_frame([_CLEAN])
        reader = FakeReader(df)
        writer = DoubleFailWriter()
        with pytest.raises(RuntimeError, match="staging error"):
            job.run(_WINDOW_START, _WINDOW_END, reader, writer)
