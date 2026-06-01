"""Unstructured event-record generation with optional defects.

Produces a list of plain Python dicts representing event payloads. A clean
record has a unique sequential id, an event type, the script's execution
timestamp (ISO-8601 string) and a random lorem-ipsum message. Because the
payloads are unstructured, each mutation is a simple operation on the dict:
nulling a value, omitting a key, adding a key or renaming a key. Mutations are
applied as independent random masks, so a single record may receive several.

The dicts are later serialised to JSON and wrapped in the Iceberg envelope by
the ``envelope`` module; this module knows nothing about Iceberg.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime

from faker import Faker

EVENT_TYPES: list[str] = ["event_one", "event_two", "event_three"]

# An event record is an unstructured mapping; keys may be added or removed.
Record = dict[str, object]


@dataclass(frozen=True)
class MutationCounts:
    """Resolved number of rows that should receive each mutation.

    Every value is an absolute row count in the range ``[0, total]``. A value of
    ``total`` means the mutation is applied to every eligible row.
    """

    total: int
    null_message: int = 0
    duplicate_id: int = 0
    extra_user_id: int = 0
    missing_event_ts: int = 0
    rename_event_type: int = 0


def _select(population: list[int], count: int, rng: random.Random) -> set[int]:
    """Return a random subset of ``population`` of the requested size.

    Args:
        population: Candidate row indices.
        count: Desired number of indices; capped at the population size.
        rng: Random source.

    Returns:
        A set of selected row indices, empty when ``count`` is not positive.
    """
    if count <= 0 or not population:
        return set()
    return set(rng.sample(population, min(count, len(population))))


def generate_records(
    mutations: MutationCounts, start_id: int, event_ts: datetime
) -> list[Record]:
    """Generate unstructured event records with any requested mutations.

    Args:
        mutations: Resolved per-mutation row counts and the total row count.
        start_id: The last id allocated by previous runs; new ids begin at
            ``start_id + 1``.
        event_ts: The script's execution timestamp, embedded in each record.

    Returns:
        A list of record dicts. Mutated records may have a nulled ``message``,
        a duplicated ``id``, an extra ``user_id`` key, no ``event_ts`` key, or
        ``event_type`` renamed to ``type``.
    """
    total = mutations.total
    rng = random.Random()
    faker = Faker()
    event_ts_iso = event_ts.isoformat()

    ids = list(range(start_id + 1, start_id + total + 1))
    base_event_types = [rng.choice(EVENT_TYPES) for _ in range(total)]
    base_messages = [faker.paragraph() for _ in range(total)]

    all_rows = list(range(total))
    null_msg = _select(all_rows, mutations.null_message, rng)
    # Row 0 stays canonical so a duplicate always collides with a real id.
    duplicates = _select(list(range(1, total)), mutations.duplicate_id, rng)
    extra = _select(all_rows, mutations.extra_user_id, rng)
    missing_ts = _select(all_rows, mutations.missing_event_ts, rng)
    renamed = _select(all_rows, mutations.rename_event_type, rng)

    records: list[Record] = []
    for i in all_rows:
        record: Record = {
            "id": ids[0] if i in duplicates else ids[i],
            "event_type": base_event_types[i],
            "event_ts": event_ts_iso,
            "message": None if i in null_msg else base_messages[i],
        }
        if i in renamed:
            record["type"] = record.pop("event_type")
        if i in missing_ts:
            record.pop("event_ts", None)
        if i in extra:
            record["user_id"] = rng.randint(1, 10_000_000)
        records.append(record)
    return records
