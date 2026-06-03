"""Generation-contract invariants for ``generate_records``.

These assert properties of the generator's *output data* that a refactor must
not change. They never touch the sink.
"""

from __future__ import annotations

import _contract_api as api


def test_total_row_count() -> None:
    """A run produces exactly the requested number of records."""
    rows = api.run(1000)
    assert len(rows) == 1000


def test_clean_run_has_canonical_shape() -> None:
    """With no mutations, every record has the four canonical keys."""
    rows = api.run(50)
    for row in rows:
        assert set(row) == {
            api.PK_FIELD,
            api.RENAMED_FROM,
            api.MISSING_FIELD,
            api.NULL_FIELD,
        }


def test_keys_unique_on_clean_run() -> None:
    """A defect-free run produces no duplicate primary keys."""
    rows = api.run(2000)
    pks = [row[api.PK_FIELD] for row in rows]
    assert len(set(pks)) == len(pks)


def test_keys_are_sequential_from_start_id() -> None:
    """Ids are contiguous, starting at ``start_id + 1``."""
    start = 1000
    rows = api.run(25, start_id=start)
    assert [row[api.PK_FIELD] for row in rows] == list(range(start + 1, start + 26))


def test_keys_do_not_repeat_across_consecutive_runs() -> None:
    """A second run starting from the advanced state never reuses keys."""
    rows1 = api.run(500)
    rows2 = api.run(500, start_id=500)
    ks1 = {row[api.PK_FIELD] for row in rows1}
    ks2 = {row[api.PK_FIELD] for row in rows2}
    assert ks1.isdisjoint(ks2)


def test_exact_null_message_count() -> None:
    """Exactly the requested number of rows have a nulled message."""
    rows = api.run(1000, null_message=50)
    nulled = sum(1 for row in rows if row.get(api.NULL_FIELD) is None)
    assert nulled == 50


def test_exact_extra_field_count() -> None:
    """Exactly the requested number of rows carry the extra ``user_id`` field."""
    rows = api.run(1000, extra_user_id=37)
    extras = sum(1 for row in rows if api.EXTRA_FIELD in row)
    assert extras == 37


def test_exact_missing_event_ts_count() -> None:
    """Exactly the requested number of rows omit ``event_ts``."""
    rows = api.run(1000, missing_event_ts=23)
    missing = sum(1 for row in rows if api.MISSING_FIELD not in row)
    assert missing == 23


def test_exact_rename_event_type_count() -> None:
    """Exactly the requested number of rows have ``event_type`` renamed."""
    rows = api.run(1000, rename_event_type=29)
    renamed = sum(
        1
        for row in rows
        if api.RENAMED_TO in row and api.RENAMED_FROM not in row
    )
    assert renamed == 29


def test_duplicate_id_collides_with_first_row() -> None:
    """``duplicate_id`` copies row 0's id onto other rows.

    K duplicates means K+1 occurrences of the canonical id (row 0 plus K
    collisions), and the unique-id count drops by exactly K.
    """
    k = 10
    rows = api.run(1000, duplicate_id=k)
    canonical_id = rows[0][api.PK_FIELD]
    occurrences = sum(1 for row in rows if row[api.PK_FIELD] == canonical_id)
    unique = {row[api.PK_FIELD] for row in rows}
    assert occurrences == k + 1
    assert len(unique) == 1000 - k


def test_mutations_clamp_at_total() -> None:
    """A count larger than ``total`` is clamped to ``total`` rather than failing."""
    rows = api.run(10, null_message=1000)
    nulled = sum(1 for row in rows if row.get(api.NULL_FIELD) is None)
    assert nulled == 10


def test_pack_rows_emits_envelope_shape() -> None:
    """``pack_rows`` yields one ``(batch_id, json, sha256, loaded_at)`` per record."""
    import hashlib
    import json

    records = api.run(5)
    loaded_at = api.make_run_ts()
    rows = list(api.pack_rows(records, batch_id="b-1", loaded_at=loaded_at))
    assert len(rows) == 5
    for record, (batch_id, payload, digest, ts) in zip(records, rows, strict=True):
        assert batch_id == "b-1"
        assert json.loads(payload) == record
        assert digest == hashlib.sha256(payload.encode("utf-8")).hexdigest()
        assert ts == loaded_at
