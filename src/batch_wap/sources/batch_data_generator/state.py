"""Persistence for the id sequence used to guarantee globally unique ids.

The generator must never reuse an id across runs. A small JSON state file holds
the last id that has been allocated; each run reads it, allocates the next block
of ids and writes the new high-water mark back once the data has been written.
"""

from __future__ import annotations

import json
from pathlib import Path


def read_last_id(state_file: Path) -> int:
    """Return the last allocated id, or 0 when no state file exists yet.

    Args:
        state_file: Path to the JSON state file.

    Returns:
        The last id allocated by a previous run, or 0 for a fresh start.
    """
    if not state_file.exists():
        return 0
    payload = json.loads(state_file.read_text(encoding="utf-8"))
    return int(payload["last_id"])


def write_last_id(state_file: Path, last_id: int) -> None:
    """Persist the last allocated id so the next run continues the sequence.

    Args:
        state_file: Path to the JSON state file.
        last_id: The highest id allocated during this run.
    """
    state_file.write_text(json.dumps({"last_id": last_id}), encoding="utf-8")
