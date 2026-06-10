"""Tests for per-row validation in models.py.

Every test drives plain dicts or JSON strings — no Polars frame needed.
Each source defect class from the contract findings (§3, §10) is covered.
"""

from __future__ import annotations

import json

import pytest

from batch_wap.ingestion.wap_v1.stage_events.models import (
    ValidationResult,
    parse_and_validate,
    validate_data,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_CLEAN = {
    "id": 1,
    "event_type": "event_one",
    "event_ts": "2026-06-05T11:50:00+00:00",
    "message": "hello world",
}


def _json(d: dict) -> str:  # noqa: D401
    """Serialise a dict as a compact JSON string."""
    return json.dumps(d)


# ---------------------------------------------------------------------------
# validate_data — clean and extra-key cases
# ---------------------------------------------------------------------------


class TestValidateDataClean:
    """A clean record passes and exposes validated fields."""

    def test_ok_true(self) -> None:
        """Clean record → ok."""
        result = validate_data(_CLEAN)
        assert result.ok is True

    def test_validated_fields(self) -> None:
        """All four required fields are present in validated."""
        result = validate_data(_CLEAN)
        assert result.validated == _CLEAN

    def test_no_extra_keys(self) -> None:
        """Clean record has no extra keys."""
        result = validate_data(_CLEAN)
        assert result.extra_keys == []

    def test_no_errors(self) -> None:
        """Clean record has empty errors."""
        result = validate_data(_CLEAN)
        assert result.errors == []
        assert result.error_count == 0


class TestValidateDataExtraUserId:
    """extra_user_id defect: extra key present but record is valid (§Defects)."""

    def test_ok_true(self) -> None:
        """extra_user_id row is valid — passes structural check."""
        data = {**_CLEAN, "user_id": 42}
        result = validate_data(data)
        assert result.ok is True

    def test_user_id_in_extra_keys(self) -> None:
        """user_id appears in extra_keys for stats collection."""
        data = {**_CLEAN, "user_id": 42}
        result = validate_data(data)
        assert "user_id" in result.extra_keys

    def test_validated_does_not_include_user_id(self) -> None:
        """Extra key is NOT in validated — it stays out of staging."""
        data = {**_CLEAN, "user_id": 42}
        result = validate_data(data)
        assert "user_id" not in result.validated


class TestValidateDataDuplicateId:
    """duplicate_id defect: a repeated integer id is structurally valid (§Defects)."""

    def test_ok_true(self) -> None:
        """A duplicate integer id still passes type/required/nullability check."""
        data = {**_CLEAN, "id": 1}  # same as row-0 id — valid int
        result = validate_data(data)
        assert result.ok is True


# ---------------------------------------------------------------------------
# validate_data — quarantine cases
# ---------------------------------------------------------------------------


class TestValidateDataNullMessage:
    """null_message defect: message=null must fail (non-null string required)."""

    def test_ok_false(self) -> None:
        """Null message → quarantine."""
        data = {**_CLEAN, "message": None}
        result = validate_data(data)
        assert result.ok is False

    def test_error_count_positive(self) -> None:
        """At least one error is reported."""
        data = {**_CLEAN, "message": None}
        result = validate_data(data)
        assert result.error_count >= 1

    def test_errors_non_empty(self) -> None:
        """Errors list is populated."""
        data = {**_CLEAN, "message": None}
        result = validate_data(data)
        assert len(result.errors) >= 1


class TestValidateDataMissingEventTs:
    """missing_event_ts defect: missing required key → quarantine."""

    def test_ok_false(self) -> None:
        """event_ts absent → quarantine."""
        data = {k: v for k, v in _CLEAN.items() if k != "event_ts"}
        result = validate_data(data)
        assert result.ok is False

    def test_error_references_event_ts(self) -> None:
        """At least one error mentions event_ts."""
        data = {k: v for k, v in _CLEAN.items() if k != "event_ts"}
        result = validate_data(data)
        error_str = json.dumps(result.errors)
        assert "event_ts" in error_str


class TestValidateDataRenameEventType:
    """rename_event_type defect: event_type absent (renamed to 'type') → quarantine."""

    def test_ok_false(self) -> None:
        """event_type absent, 'type' extra → quarantine."""
        data = {k: v for k, v in _CLEAN.items() if k != "event_type"}
        data["type"] = "event_one"  # renamed field
        result = validate_data(data)
        assert result.ok is False

    def test_type_appears_as_extra_key(self) -> None:
        """The 'type' rename-target key may appear in extra_keys even on failure."""
        # On failure, extra_keys is empty (we only expose model_extra on success).
        # This test documents the contract: extra_keys is not populated for failed rows.
        data = {k: v for k, v in _CLEAN.items() if k != "event_type"}
        data["type"] = "event_one"
        result = validate_data(data)
        assert result.ok is False
        # extra_keys is empty for failed validation (caller uses original envelope)
        assert result.extra_keys == []


class TestValidateDataStrictMode:
    """strict=True: no type coercion allowed."""

    def test_string_id_fails(self) -> None:
        """id='5' (string) must not coerce to 5 — strict mode rejects it."""
        data = {**_CLEAN, "id": "5"}
        result = validate_data(data)
        assert result.ok is False

    def test_integer_id_passes(self) -> None:
        """id=5 (integer) passes strict mode."""
        data = {**_CLEAN, "id": 5}
        result = validate_data(data)
        assert result.ok is True


# ---------------------------------------------------------------------------
# parse_and_validate — JSON layer
# ---------------------------------------------------------------------------


class TestParseAndValidate:
    """parse_and_validate wraps JSON parsing before Pydantic validation."""

    def test_valid_json_clean_record(self) -> None:
        """A clean JSON string passes."""
        result = parse_and_validate(_json(_CLEAN))
        assert result.ok is True

    def test_invalid_json_is_quarantined(self) -> None:
        """Non-JSON string → parse error, ok=False."""
        result = parse_and_validate("not json at all {{{")
        assert result.ok is False
        assert result.error_count >= 1

    def test_parse_error_type_label(self) -> None:
        """The synthetic parse error carries a 'json_parse_error' type label."""
        result = parse_and_validate("not json")
        assert result.errors[0]["type"] == "json_parse_error"

    def test_null_message_via_json(self) -> None:
        """JSON-encoded null message is quarantined end-to-end."""
        data = {**_CLEAN, "message": None}
        result = parse_and_validate(_json(data))
        assert result.ok is False

    def test_extra_user_id_via_json(self) -> None:
        """JSON-encoded extra user_id passes end-to-end."""
        data = {**_CLEAN, "user_id": 999}
        result = parse_and_validate(_json(data))
        assert result.ok is True
        assert "user_id" in result.extra_keys


# ---------------------------------------------------------------------------
# ValidationResult is immutable (frozen dataclass)
# ---------------------------------------------------------------------------


class TestValidationResultImmutable:
    """ValidationResult is frozen — mutation raises."""

    def test_frozen(self) -> None:
        """Assigning to a field of a ValidationResult raises."""
        result = ValidationResult(ok=True)
        with pytest.raises((AttributeError, TypeError)):
            result.ok = False  # type: ignore[misc]
