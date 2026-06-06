"""Pytest fixtures for the WAP v1 test suite.

Shared helpers (FakeReader, FakeWriter, make_source_frame, contract-name
constants) live in ``_helpers.py`` and are imported directly by test modules
as ``import _helpers`` — matching the ``import _contract_api`` convention used
in ``tests/batch_data_generator/``.

This file exposes only pytest fixtures that wire those helpers into tests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import _helpers
import polars as pl
import pytest

# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

_WINDOW_START = datetime(2026, 6, 5, 11, 50, 0, tzinfo=timezone.utc)
_WINDOW_END = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def window_bounds() -> tuple[datetime, datetime]:
    """Return a canonical (window_start, window_end) pair for tests."""
    return _WINDOW_START, _WINDOW_END


@pytest.fixture()
def clean_record() -> dict[str, Any]:
    """Return a single clean event record dict (no defects)."""
    return {
        "id": 1,
        "event_type": "event_one",
        "event_ts": "2026-06-05T11:50:00+00:00",
        "message": "hello world",
    }


@pytest.fixture()
def fake_writer() -> _helpers.FakeWriter:
    """Return a fresh FakeWriter."""
    return _helpers.FakeWriter()


@pytest.fixture()
def empty_source_frame() -> pl.DataFrame:
    """Return an empty source frame with the correct schema."""
    return _helpers.make_source_frame([])
