"""Packs unstructured records into the ClickHouse envelope row shape.

Each generated record is serialised to a JSON string and emitted as a four-field
tuple matching the target ClickHouse table columns:

* ``batch_id``    - identifier shared by every row produced in one run
* ``data``        - the record serialised as a JSON string
* ``hashed_json`` - SHA-256 hex digest of the ``data`` string
* ``loaded_at``   - the script's execution timestamp (UTC ``datetime``)
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from datetime import datetime

from batch_wap.sources.batch_data_generator.generator import Record


def pack_rows(
    records: Iterable[Record], batch_id: str, loaded_at: datetime
) -> Iterator[tuple[str, str, str, datetime]]:
    """Serialise records and yield them as envelope row tuples.

    Args:
        records: The unstructured event payloads.
        batch_id: Identifier assigned to every row in this run.
        loaded_at: The script's execution timestamp (UTC).

    Yields:
        Tuples of ``(batch_id, data, hashed_json, loaded_at)`` in input order.
    """
    for record in records:
        payload = json.dumps(record)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        yield (batch_id, payload, digest, loaded_at)
