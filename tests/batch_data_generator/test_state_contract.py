"""State-contract invariants for the id sequence persistence.

Covers the round-trip and the missing-file behavior. No sink involved.
"""

from __future__ import annotations

import pathlib

import _contract_api as api


def test_missing_file_returns_zero(tmp_path: pathlib.Path) -> None:
    """Loading a non-existent state file yields 0, not an error."""
    assert api.read_last_id(tmp_path / "missing.json") == 0


def test_write_then_read_round_trip(tmp_path: pathlib.Path) -> None:
    """The last id written reloads to the same integer."""
    path = tmp_path / "state.json"
    api.write_last_id(path, 1234)
    assert api.read_last_id(path) == 1234


def test_overwrite_replaces_previous_value(tmp_path: pathlib.Path) -> None:
    """A later write replaces the earlier value cleanly."""
    path = tmp_path / "state.json"
    api.write_last_id(path, 10)
    api.write_last_id(path, 25)
    assert api.read_last_id(path) == 25


def test_state_drives_next_run_start(tmp_path: pathlib.Path) -> None:
    """Two runs chained through the state file produce contiguous, unique ids."""
    path = tmp_path / "state.json"
    first = api.run(100, start_id=api.read_last_id(path))
    api.write_last_id(path, 100)
    second = api.run(100, start_id=api.read_last_id(path))
    ids = [row[api.PK_FIELD] for row in first + second]
    assert ids == list(range(1, 201))
