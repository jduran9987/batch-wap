"""Packs unstructured records into the Iceberg envelope table.

The target Iceberg table stores each generated row as an opaque JSON string
alongside run metadata. This module serialises the records, hashes each JSON
payload and assembles the four-column PyArrow table that the sink appends:

* ``batch_id``    - identifier shared by every row produced in one run
* ``data``        - the record serialised as a JSON string
* ``hashed_json`` - SHA-256 hex digest of the ``data`` string
* ``loaded_at``   - the script's execution timestamp
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

import pyarrow as pa

from batch_wap.sources.batch_data_generator.generator import Record


def pack(records: list[Record], batch_id: str, loaded_at: datetime) -> pa.Table:
    """Serialise records and build the four-column envelope table.

    Args:
        records: The unstructured event payloads.
        batch_id: Identifier assigned to every row in this run.
        loaded_at: The script's execution timestamp.

    Returns:
        A PyArrow table with columns ``batch_id``, ``data``, ``hashed_json`` and
        ``loaded_at``.
    """
    data_strings = [json.dumps(record) for record in records]
    hashes = [
        hashlib.sha256(payload.encode("utf-8")).hexdigest() for payload in data_strings
    ]
    row_count = len(records)
    return pa.table(
        {
            "batch_id": pa.array([batch_id] * row_count, type=pa.string()),
            "data": pa.array(data_strings, type=pa.string()),
            "hashed_json": pa.array(hashes, type=pa.string()),
            "loaded_at": pa.array(
                [loaded_at] * row_count, type=pa.timestamp("us", tz="UTC")
            ),
        }
    )
