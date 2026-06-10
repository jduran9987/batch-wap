"""Per-row validation for WAP v1 raw event records.

Validation is strictly structural: type checking, required-field enforcement,
and nullability — no business-rule checks (no enum membership, no ISO-format
check, no range validation).  Extra keys are allowed and surfaced for stats; they
do **not** fail a record.

A row that cannot be parsed as JSON is quarantined immediately with a synthetic
parse-error result rather than being passed to the Pydantic model.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator


class RawEvent(BaseModel):
    """Strict pydantic model for a clean raw event record.

    Fields must match by type exactly (``strict=True``); no coercion is
    performed.  Extra keys are permitted and stored in ``model_extra``; they do
    not cause a validation failure.
    """

    model_config = ConfigDict(strict=True, extra="allow")

    id: int
    event_type: str
    event_ts: str
    message: str

    @field_validator("event_ts")
    @classmethod
    def _check_event_ts_is_datetime(cls, value: str) -> str:
        """Ensure ``event_ts`` is a datetime-parseable string.

        The field stays a ``str`` (so it round-trips unchanged into
        ClickHouse), but its value must be parseable as an ISO-8601 datetime,
        since downstream jobs convert it to a ClickHouse ``DateTime``.
        """
        try:
            datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"event_ts is not a valid datetime string: {value!r}") from exc
        return value


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating a single ``data`` JSON string.

    Attributes:
        ok: ``True`` when the record passed validation.
        validated: The validated field values (populated only when ``ok``).
        extra_keys: Names of unexpected keys present in the record.  Populated
            even when ``ok`` is ``False`` so the caller can log them.
        errors: List of Pydantic or parse error dicts (populated when ``ok``
            is ``False``).
        error_count: ``len(errors)``; convenience attribute.
    """

    ok: bool
    validated: dict[str, Any] = field(default_factory=dict)
    extra_keys: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    error_count: int = 0


def validate_data(data: dict[str, Any]) -> ValidationResult:
    """Validate a pre-parsed record dict against the ``RawEvent`` model.

    Args:
        data: A Python dict decoded from the ``data`` JSON column.

    Returns:
        A :class:`ValidationResult` with ``ok=True`` and the validated field
        values on success, or ``ok=False`` with structured error details on
        failure.
    """
    try:
        event = RawEvent.model_validate(data)
    except ValidationError as exc:
        errors = exc.errors()
        return ValidationResult(
            ok=False,
            extra_keys=[],
            errors=errors,
            error_count=len(errors),
        )
    extra_keys = sorted(event.model_extra.keys()) if event.model_extra else []
    return ValidationResult(
        ok=True,
        validated={
            "id": event.id,
            "event_type": event.event_type,
            "event_ts": event.event_ts,
            "message": event.message,
        },
        extra_keys=extra_keys,
        errors=[],
        error_count=0,
    )


def parse_and_validate(raw_json: str) -> ValidationResult:
    """Parse ``raw_json`` and validate it as a :class:`RawEvent`.

    If the string is not valid JSON the row is quarantined immediately with a
    synthetic parse-error result.  Otherwise the decoded dict is passed to
    :func:`validate_data`.

    Args:
        raw_json: The raw ``data`` column value from the source table.

    Returns:
        A :class:`ValidationResult`; ``ok=False`` for both parse errors and
        Pydantic validation failures.
    """
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        parse_error: dict[str, Any] = {
            "type": "json_parse_error",
            "msg": str(exc),
            "input": raw_json,
        }
        return ValidationResult(
            ok=False,
            extra_keys=[],
            errors=[parse_error],
            error_count=1,
        )
    return validate_data(data)
