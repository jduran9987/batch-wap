"""Tests for the ClickHouse sink.

Unlike the generation/state contract tests, these exercise ``sink.py`` directly.
The warehouse is replaced with an in-memory fake client (and ``time.sleep`` is
neutralised), so the batching, table-creation, and retry-backoff behaviour can
be asserted without a running ClickHouse.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from batch_wap.sources.batch_data_generator import sink as sink_mod
from batch_wap.sources.batch_data_generator.sink import (
    COLUMN_NAMES,
    COLUMN_TYPES,
    ClickHouseSink,
    _chunked,
    _create_table_ddl,
)

Row = tuple[str, str, str, datetime]


def _make_row(batch_id: str = "b-1") -> Row:
    """Build a single envelope row tuple for tests.

    Args:
        batch_id: The batch id to embed in the row.

    Returns:
        A ``(batch_id, data, hashed_json, loaded_at)`` tuple.
    """
    return (batch_id, "{}", "deadbeef", datetime(2026, 1, 1, tzinfo=timezone.utc))


class FakeClient:
    """In-memory stand-in for a ``clickhouse-connect`` client.

    Records every DDL command and insert so tests can assert on them, and can
    be configured to fail a fixed number of inserts before succeeding.
    """

    def __init__(self, fail_times: int = 0, fail_forever: bool = False) -> None:
        """Initialise the fake client.

        Args:
            fail_times: Number of leading ``insert`` calls that raise.
            fail_forever: When true, every ``insert`` call raises.
        """
        self.commands: list[str] = []
        self.inserts: list[list[tuple]] = []
        self.closed = False
        self._fail_times = fail_times
        self._fail_forever = fail_forever
        self._insert_calls = 0

    def command(self, statement: str) -> None:
        """Record a DDL command.

        Args:
            statement: The SQL statement issued by the sink.
        """
        self.commands.append(statement)

    def insert(
        self,
        *,
        table: str,
        data: list[tuple],
        column_names: list[str],
        column_type_names: list[str],
        database: str,
    ) -> None:
        """Record an insert, raising while the configured failure budget holds.

        Args:
            table: Target table name.
            data: The batch of rows.
            column_names: Column names for the insert.
            column_type_names: Column types for the insert.
            database: Target database name.

        Raises:
            RuntimeError: While the fake is configured to fail.
        """
        self._insert_calls += 1
        if self._fail_forever or self._insert_calls <= self._fail_times:
            raise RuntimeError("transient insert failure")
        self.inserts.append(list(data))

    def close(self) -> None:
        """Mark the client as closed."""
        self.closed = True


def _make_sink(
    monkeypatch: pytest.MonkeyPatch, client: FakeClient, **overrides: Any
) -> ClickHouseSink:
    """Construct a sink wired to ``client`` instead of a real ClickHouse.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        client: The fake client to return from ``get_client``.
        **overrides: Constructor argument overrides.

    Returns:
        A ``ClickHouseSink`` backed by the fake client.
    """
    monkeypatch.setattr(
        sink_mod.clickhouse_connect, "get_client", lambda **_kwargs: client
    )
    params: dict[str, Any] = {
        "host": "localhost",
        "port": 8123,
        "username": "default",
        "password": "",
        "database": "db",
        "table": "raw",
        "batch_size": 2,
    }
    params.update(overrides)
    return ClickHouseSink(**params)


def test_chunked_splits_into_fixed_size_batches() -> None:
    """``_chunked`` yields full batches then a smaller final remainder."""
    rows = [(_i,) for _i in range(5)]
    batches = list(_chunked(rows, 2))
    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert [row for batch in batches for row in batch] == rows


def test_chunked_empty_input_yields_nothing() -> None:
    """``_chunked`` over an empty iterable yields no batches."""
    assert list(_chunked([], 10)) == []


def test_create_table_ddl_matches_envelope_schema() -> None:
    """The DDL is an idempotent ``MergeTree`` create with the four envelope columns."""
    ddl = _create_table_ddl("db", "raw")
    assert "CREATE TABLE IF NOT EXISTS `db`.`raw`" in ddl
    assert "batch_id    String" in ddl
    assert "data        String" in ddl
    assert "hashed_json String" in ddl
    assert "loaded_at   DateTime64(6, 'UTC')" in ddl
    assert "ENGINE = MergeTree" in ddl
    assert "ORDER BY (loaded_at, batch_id)" in ddl
    assert "PARTITION BY toYYYYMM(loaded_at)" in ddl


def test_non_positive_batch_size_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-positive ``batch_size`` raises before any connection is made."""
    with pytest.raises(ValueError):
        _make_sink(monkeypatch, FakeClient(), batch_size=0)
    with pytest.raises(ValueError):
        _make_sink(monkeypatch, FakeClient(), batch_size=-1)


def test_write_creates_table_then_inserts_in_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``write`` creates the table once, inserts in batches, and returns the count."""
    client = FakeClient()
    sink = _make_sink(monkeypatch, client, batch_size=2)
    rows = [_make_row() for _ in range(5)]

    written = sink.write(rows)

    assert written == 5
    assert len(client.commands) == 1
    assert "CREATE TABLE IF NOT EXISTS" in client.commands[0]
    assert [len(batch) for batch in client.inserts] == [2, 2, 1]


def test_write_uses_declared_column_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inserts carry exactly the envelope column names and types."""
    captured: dict[str, Any] = {}

    class CapturingClient(FakeClient):
        def insert(self, **kwargs: Any) -> None:
            captured.update(kwargs)
            super().insert(**kwargs)

    sink = _make_sink(monkeypatch, CapturingClient(), batch_size=10)
    sink.write([_make_row()])

    assert captured["column_names"] == COLUMN_NAMES
    assert captured["column_type_names"] == COLUMN_TYPES
    assert captured["table"] == "raw"
    assert captured["database"] == "db"


def test_write_retries_transient_failures_with_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A batch that fails twice is retried and succeeds, with exponential backoff."""
    sleeps: list[float] = []
    monkeypatch.setattr(sink_mod.time, "sleep", lambda seconds: sleeps.append(seconds))

    client = FakeClient(fail_times=2)
    sink = _make_sink(
        monkeypatch, client, batch_size=10, max_retries=3, retry_backoff_seconds=1.0
    )

    written = sink.write([_make_row(), _make_row()])

    assert written == 2
    assert len(client.inserts) == 1
    # Two failures -> two backoff sleeps: 1.0 * 2**0, 1.0 * 2**1.
    assert sleeps == [1.0, 2.0]


def test_write_raises_after_exhausting_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When every attempt fails, the last error propagates and nothing is inserted."""
    monkeypatch.setattr(sink_mod.time, "sleep", lambda _seconds: None)

    client = FakeClient(fail_forever=True)
    sink = _make_sink(monkeypatch, client, batch_size=10, max_retries=3)

    with pytest.raises(RuntimeError, match="transient insert failure"):
        sink.write([_make_row()])
    assert client.inserts == []


def test_close_closes_underlying_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close`` closes the underlying ClickHouse client."""
    client = FakeClient()
    sink = _make_sink(monkeypatch, client)
    sink.close()
    assert client.closed is True
