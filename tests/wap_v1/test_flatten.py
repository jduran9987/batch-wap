"""Tests for flatten.py — spec test #4.

Valid records must yield a Polars DataFrame with exactly the staging columns,
correct dtypes (id Int64, timestamps Datetime(us, UTC)), hashed_json and
lineage carried through, and extra keys absent from the frame.
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from batch_wap.ingestion.wap_v1.flatten import flatten_valid

_UTC = timezone.utc
_JOB_RUN_ID = "w202606051150"

_LOADED_AT = datetime(2026, 6, 5, 11, 50, 0, tzinfo=_UTC)


def _make_row(
    *,
    id: int = 1,
    event_type: str = "event_one",
    event_ts: str = "2026-06-05T11:50:00+00:00",
    message: str = "hello",
    hashed_json: str = "abc123",
    batch_id: str = "batch-1",
    loaded_at: datetime = _LOADED_AT,
    **extra: object,
) -> dict:
    """Build a good-row dict as produced by the validation step."""
    row = {
        "id": id,
        "event_type": event_type,
        "event_ts": event_ts,
        "message": message,
        "hashed_json": hashed_json,
        "batch_id": batch_id,
        "loaded_at": loaded_at,
    }
    row.update(extra)
    return row


_EXPECTED_COLUMNS = {
    "id",
    "event_type",
    "event_ts",
    "message",
    "hashed_json",
    "batch_id",
    "source_loaded_at",
    "job_run_id",
}


class TestFlattenValidColumns:
    """Column set is exactly the staging insert columns."""

    def test_column_names(self) -> None:
        """Frame has exactly the expected staging columns."""
        df = flatten_valid([_make_row()], _JOB_RUN_ID)
        assert set(df.columns) == _EXPECTED_COLUMNS

    def test_no_extra_columns(self) -> None:
        """Extra keys in the input dict do not appear in the frame."""
        row = _make_row(user_id=42, unexpected_field="drop me")
        df = flatten_valid([row], _JOB_RUN_ID)
        assert "user_id" not in df.columns
        assert "unexpected_field" not in df.columns

    def test_loaded_at_renamed_to_source_loaded_at(self) -> None:
        """loaded_at is carried as source_loaded_at, not loaded_at."""
        df = flatten_valid([_make_row()], _JOB_RUN_ID)
        assert "source_loaded_at" in df.columns
        assert "loaded_at" not in df.columns


class TestFlattenValidDtypes:
    """Column dtypes match the staging schema."""

    def test_id_is_int64(self) -> None:
        """Id column is Int64."""
        df = flatten_valid([_make_row()], _JOB_RUN_ID)
        assert df.schema["id"] == pl.Int64

    def test_source_loaded_at_is_datetime_utc(self) -> None:
        """source_loaded_at is Datetime(us, UTC)."""
        df = flatten_valid([_make_row()], _JOB_RUN_ID)
        dtype = df.schema["source_loaded_at"]
        assert isinstance(dtype, pl.Datetime)
        assert dtype.time_zone == "UTC"

    def test_string_columns(self) -> None:
        """String columns have the String dtype."""
        df = flatten_valid([_make_row()], _JOB_RUN_ID)
        str_cols = (
            "event_type", "event_ts", "message",
            "hashed_json", "batch_id", "job_run_id",
        )
        for col in str_cols:
            assert df.schema[col] == pl.String, f"{col} should be String"


class TestFlattenValidValues:
    """Values are carried correctly from the input dict."""

    def test_id_value(self) -> None:
        """Id value is preserved."""
        df = flatten_valid([_make_row(id=7)], _JOB_RUN_ID)
        assert df["id"][0] == 7

    def test_hashed_json_carried(self) -> None:
        """hashed_json is carried from source lineage."""
        df = flatten_valid([_make_row(hashed_json="deadbeef")], _JOB_RUN_ID)
        assert df["hashed_json"][0] == "deadbeef"

    def test_batch_id_carried(self) -> None:
        """batch_id lineage is carried."""
        df = flatten_valid([_make_row(batch_id="b-42")], _JOB_RUN_ID)
        assert df["batch_id"][0] == "b-42"

    def test_job_run_id_attached(self) -> None:
        """job_run_id is attached to every row."""
        df = flatten_valid([_make_row(), _make_row(id=2)], _JOB_RUN_ID)
        assert df["job_run_id"].to_list() == [_JOB_RUN_ID, _JOB_RUN_ID]

    def test_source_loaded_at_value(self) -> None:
        """source_loaded_at reflects the original loaded_at datetime."""
        df = flatten_valid([_make_row(loaded_at=_LOADED_AT)], _JOB_RUN_ID)
        # Polars returns a datetime; compare via isoformat
        val = df["source_loaded_at"][0]
        assert val is not None


class TestFlattenValidEdgeCases:
    """Edge cases: empty input, multi-row frame."""

    def test_empty_input_returns_correct_schema(self) -> None:
        """Empty input returns a zero-row frame with the correct schema."""
        df = flatten_valid([], _JOB_RUN_ID)
        assert df.height == 0
        assert set(df.columns) == _EXPECTED_COLUMNS
        assert df.schema["id"] == pl.Int64

    def test_multi_row_frame_height(self) -> None:
        """Multiple input rows produce the correct row count."""
        rows = [_make_row(id=i) for i in range(1, 6)]
        df = flatten_valid(rows, _JOB_RUN_ID)
        assert df.height == 5

    def test_row_count_matches_input(self) -> None:
        """Frame height equals the number of input dicts."""
        rows = [_make_row(id=i) for i in range(1, 4)]
        df = flatten_valid(rows, _JOB_RUN_ID)
        assert df.height == len(rows)
