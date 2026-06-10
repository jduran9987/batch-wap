"""Spec test #3 — quarantine deduplication.

Two bad rows sharing the same hashed_json in one window must result in a
single quarantined row.  This covers the "no duplicate bad rows" requirement:
within a run, bad rows are deduped on hashed_json in Polars before the
partition-replace write.

No DB client is constructed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import _helpers

from batch_wap.ingestion.wap_v1.stage_events import job

NULL_FIELD = _helpers.NULL_FIELD
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


class TestQuarantineDedup:
    """Duplicate bad rows on hashed_json collapse to one quarantined row."""

    def test_two_identical_bad_rows_collapse_to_one(self) -> None:
        """Two bad rows with the same data (same hashed_json) → one quarantined row."""
        bad_data = {**_CLEAN, NULL_FIELD: None}
        # make_source_frame with the same dict twice → identical data strings →
        # identical hashed_json values → dedup removes one.
        df = make_source_frame([bad_data, bad_data])
        reader = FakeReader(df)
        writer = FakeWriter()
        job.run(_WINDOW_START, _WINDOW_END, reader, writer)
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 1

    def test_two_distinct_bad_rows_both_quarantined(self) -> None:
        """Two distinct bad rows (different hashed_json) → two quarantined rows."""
        bad1 = {**_CLEAN, NULL_FIELD: None}
        bad2 = {**_CLEAN, "id": 2, NULL_FIELD: None}
        df = make_source_frame([bad1, bad2])
        reader = FakeReader(df)
        writer = FakeWriter()
        job.run(_WINDOW_START, _WINDOW_END, reader, writer)
        _, qdf = writer.quarantine_calls[0]
        assert qdf.height == 2

    def test_dedup_does_not_affect_staging(self) -> None:
        """Dedup only touches quarantine; good rows in the same run are unaffected."""
        bad_data = {**_CLEAN, NULL_FIELD: None}
        good_data = {**_CLEAN, "id": 2}
        df = make_source_frame([bad_data, bad_data, good_data])
        reader = FakeReader(df)
        writer = FakeWriter()
        job.run(_WINDOW_START, _WINDOW_END, reader, writer)
        _, qdf = writer.quarantine_calls[0]
        _, sdf = writer.staging_calls[0]
        assert qdf.height == 1  # deduped
        assert sdf.height == 1  # untouched

    def test_quarantined_row_count_in_stats(self) -> None:
        """Stats rows_quarantined reflects post-dedup count."""
        bad_data = {**_CLEAN, NULL_FIELD: None}
        df = make_source_frame([bad_data, bad_data])
        reader = FakeReader(df)
        writer = FakeWriter()
        job.run(_WINDOW_START, _WINDOW_END, reader, writer)
        assert writer.stats_rows[0]["rows_quarantined"] == 1
