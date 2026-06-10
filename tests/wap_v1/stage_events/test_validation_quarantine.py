"""Spec test #1 — quarantine routing.

Rows with null_message, missing_event_ts, and rename_event_type defects must
be routed to replace_quarantine_partition with the original envelope preserved
and non-empty validation_errors / error_count.  None of these rows should
reach staging.

No DB client is constructed; all I/O goes through FakeReader / FakeWriter.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import _helpers

from batch_wap.ingestion.wap_v1.stage_events import job

EXTRA_FIELD = _helpers.EXTRA_FIELD
MISSING_FIELD = _helpers.MISSING_FIELD
NULL_FIELD = _helpers.NULL_FIELD
RENAMED_FROM = _helpers.RENAMED_FROM
RENAMED_TO = _helpers.RENAMED_TO
FakeReader = _helpers.FakeReader
FakeWriter = _helpers.FakeWriter
make_source_frame = _helpers.make_source_frame

_UTC = timezone.utc
_WINDOW_START = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)
_WINDOW_END = datetime(2026, 6, 5, 12, 0, 0, tzinfo=_UTC)
_JOB_RUN_ID = "w202606051150"

_CLEAN = {
    "id": 1,
    "event_type": "event_one",
    "event_ts": "2026-06-05T11:50:00+00:00",
    "message": "hello",
}


def _run(data_dicts: list[dict]) -> FakeWriter:
    """Run the job with the given data dicts and return the writer."""
    df = make_source_frame(data_dicts)
    reader = FakeReader(df)
    writer = FakeWriter()
    job.run(_WINDOW_START, _WINDOW_END, reader, writer)
    return writer


class TestNullMessageIsQuarantined:
    """null_message defect → quarantine, not staging."""

    def test_reaches_quarantine(self) -> None:
        """Null-message row reaches quarantine."""
        data = {**_CLEAN, NULL_FIELD: None}
        writer = _run([data])
        assert writer.quarantine_calls
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 1

    def test_does_not_reach_staging(self) -> None:
        """Null-message row is not written to staging."""
        data = {**_CLEAN, NULL_FIELD: None}
        writer = _run([data])
        _, sdf = writer.staging_calls[0]
        assert sdf.height == 0

    def test_original_envelope_preserved(self) -> None:
        """The original data string is stored verbatim in quarantine."""
        data = {**_CLEAN, NULL_FIELD: None}
        df = make_source_frame([data])
        reader = FakeReader(df)
        writer = FakeWriter()
        job.run(_WINDOW_START, _WINDOW_END, reader, writer)
        _, qdf = writer.quarantine_calls[0]
        assert qdf["data"][0] == df["data"][0]
        assert qdf["batch_id"][0] == df["batch_id"][0]
        assert qdf["hashed_json"][0] == df["hashed_json"][0]

    def test_validation_errors_non_empty(self) -> None:
        """validation_errors carries at least one error dict."""
        data = {**_CLEAN, NULL_FIELD: None}
        writer = _run([data])
        _, qdf = writer.quarantine_calls[0]
        errors = json.loads(qdf["validation_errors"][0])
        assert len(errors) >= 1

    def test_error_count_positive(self) -> None:
        """error_count is at least 1."""
        data = {**_CLEAN, NULL_FIELD: None}
        writer = _run([data])
        _, qdf = writer.quarantine_calls[0]
        assert qdf["error_count"][0] >= 1


class TestMissingEventTsIsQuarantined:
    """missing_event_ts defect → quarantine, not staging."""

    def test_reaches_quarantine(self) -> None:
        """Row with missing event_ts reaches quarantine."""
        data = {k: v for k, v in _CLEAN.items() if k != MISSING_FIELD}
        writer = _run([data])
        assert writer.quarantine_calls
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 1

    def test_does_not_reach_staging(self) -> None:
        """Row with missing event_ts is not written to staging."""
        data = {k: v for k, v in _CLEAN.items() if k != MISSING_FIELD}
        writer = _run([data])
        _, sdf = writer.staging_calls[0]
        assert sdf.height == 0

    def test_validation_errors_mention_event_ts(self) -> None:
        """Error detail references the missing event_ts field."""
        data = {k: v for k, v in _CLEAN.items() if k != MISSING_FIELD}
        writer = _run([data])
        _, qdf = writer.quarantine_calls[0]
        errors_str = qdf["validation_errors"][0]
        assert "event_ts" in errors_str

    def test_error_count_positive(self) -> None:
        """error_count is at least 1."""
        data = {k: v for k, v in _CLEAN.items() if k != MISSING_FIELD}
        writer = _run([data])
        _, qdf = writer.quarantine_calls[0]
        assert qdf["error_count"][0] >= 1


class TestRenameEventTypeIsQuarantined:
    """rename_event_type defect → quarantine (event_type missing, type present)."""

    def test_reaches_quarantine(self) -> None:
        """Row with renamed event_type reaches quarantine."""
        data = {k: v for k, v in _CLEAN.items() if k != RENAMED_FROM}
        data[RENAMED_TO] = "event_one"
        writer = _run([data])
        assert writer.quarantine_calls
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 1

    def test_does_not_reach_staging(self) -> None:
        """Row with renamed event_type is not written to staging."""
        data = {k: v for k, v in _CLEAN.items() if k != RENAMED_FROM}
        data[RENAMED_TO] = "event_one"
        writer = _run([data])
        _, sdf = writer.staging_calls[0]
        assert sdf.height == 0

    def test_validation_errors_non_empty(self) -> None:
        """At least one error is reported for renamed event_type."""
        data = {k: v for k, v in _CLEAN.items() if k != RENAMED_FROM}
        data[RENAMED_TO] = "event_one"
        writer = _run([data])
        _, qdf = writer.quarantine_calls[0]
        errors = json.loads(qdf["validation_errors"][0])
        assert len(errors) >= 1


class TestJobRunIdOnQuarantineRows:
    """Quarantined rows carry the correct job_run_id."""

    def test_job_run_id_attached(self) -> None:
        """job_run_id derived from window_start is attached to quarantined rows."""
        data = {**_CLEAN, NULL_FIELD: None}
        writer = _run([data])
        _, qdf = writer.quarantine_calls[0]
        assert qdf["job_run_id"][0] == _JOB_RUN_ID


class TestMixedGoodAndBadRows:
    """Mixed window: bad rows quarantined, good rows staged."""

    def test_counts_split_correctly(self) -> None:
        """One bad + one good → one quarantined, one staged."""
        bad = {**_CLEAN, NULL_FIELD: None}
        good = {**_CLEAN, "id": 2}
        writer = _run([bad, good])
        _, qdf = writer.quarantine_calls[0]
        _, sdf = writer.staging_calls[0]
        assert qdf.height == 1
        assert sdf.height == 1
