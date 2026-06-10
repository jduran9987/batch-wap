"""Tests for the job_run_id derivation in ids.py.

The job_run_id is the idempotency lynchpin of the WAP v1 design: a retrigger
of the same window must compute the exact same id so the partition-replace
path replaces the prior output rather than duplicating it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from batch_wap.ingestion.wap_v1.stage_events.ids import derive_job_run_id

_UTC = timezone.utc


class TestDeriveJobRunId:
    """Correctness and safety tests for derive_job_run_id."""

    def test_spec_example_utc(self) -> None:
        """[11:50, 12:00) UTC window -> 'w202606051150' (spec worked example)."""
        window_start = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)
        assert derive_job_run_id(window_start) == "w202606051150"

    def test_non_utc_tz_normalized_to_utc(self) -> None:
        """A non-UTC tz-aware input produces the same id as its UTC equivalent."""
        # UTC+05:30 (IST): 17:20 IST == 11:50 UTC
        ist = timezone(timedelta(hours=5, minutes=30))
        window_start_ist = datetime(2026, 6, 5, 17, 20, 0, tzinfo=ist)
        window_start_utc = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)
        assert derive_job_run_id(window_start_ist) == derive_job_run_id(
            window_start_utc
        )

    def test_naive_datetime_raises(self) -> None:
        """A naive datetime is rejected with a clear ValueError."""
        naive = datetime(2026, 6, 5, 11, 50, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            derive_job_run_id(naive)

    def test_determinism(self) -> None:
        """Two calls with the same window_start return the same id."""
        window_start = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)
        assert derive_job_run_id(window_start) == derive_job_run_id(window_start)

    def test_adjacent_windows_differ(self) -> None:
        """Adjacent 10-minute windows produce different ids."""
        w1 = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)
        w2 = w1 + timedelta(minutes=10)
        assert derive_job_run_id(w1) != derive_job_run_id(w2)
        assert derive_job_run_id(w2) == "w202606051200"

    def test_id_starts_with_w(self) -> None:
        """job_run_id always begins with 'w'."""
        window_start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=_UTC)
        assert derive_job_run_id(window_start).startswith("w")

    def test_id_length(self) -> None:
        """job_run_id is 'w' + 12-char timestamp = 13 chars total."""
        window_start = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)
        assert len(derive_job_run_id(window_start)) == 13
