"""Command-line interface for the batch event-data generator.

Parses arguments, generates unstructured event records (with any requested
mutations), packs them into the envelope row shape, and appends them in batches
to a ClickHouse table. The persisted id sequence is advanced only after every
batch insert succeeds. The mutation flags accept either an integer or the
literal ``ALL``, where ``ALL`` resolves to the total row count.

ClickHouse connection settings are sourced from CLI flags with environment
variable fallbacks so secrets are never hardcoded.
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

from batch_wap.sources.batch_data_generator.envelope import pack_rows
from batch_wap.sources.batch_data_generator.generator import (
    MutationCounts,
    generate_records,
)
from batch_wap.sources.batch_data_generator.sink import ClickHouseSink
from batch_wap.sources.batch_data_generator.state import read_last_id, write_last_id

_MUTATION_FLAGS: list[str] = [
    "null-message",
    "duplicate-id",
    "extra-user-id",
    "missing-event-ts",
    "rename-event-type",
]

DEFAULT_BATCH_SIZE: int = 50_000


def _int_or_all(value: str) -> Union[int, str]:
    """Argparse type accepting a non-negative integer or the literal 'ALL'.

    Args:
        value: Raw command-line value.

    Returns:
        The string ``"ALL"`` or a non-negative integer.

    Raises:
        argparse.ArgumentTypeError: If the value is negative or not an int/ALL.
    """
    if value.upper() == "ALL":
        return "ALL"
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("count must be non-negative")
    return number


def _resolve(value: Union[int, str], total: int) -> int:
    """Resolve an int-or-'ALL' value into an absolute row count.

    Args:
        value: A parsed mutation flag value.
        total: The total number of rows being generated.

    Returns:
        ``total`` when ``value`` is ``"ALL"``, otherwise the integer value.
    """
    return total if value == "ALL" else int(value)


def _split_table(identifier: str) -> tuple[str, str]:
    """Split a ``database.table`` identifier into its two parts.

    Args:
        identifier: A ClickHouse identifier in ``database.table`` form.

    Returns:
        A ``(database, table)`` pair.

    Raises:
        argparse.ArgumentTypeError: If the identifier is not exactly two dot
            separated, non-empty parts.
    """
    parts = identifier.split(".")
    if len(parts) != 2 or not all(parts):
        raise argparse.ArgumentTypeError(
            f"--table must be 'database.table', got {identifier!r}"
        )
    return parts[0], parts[1]


def _parse_args(argv: Union[list[str], None] = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Optional argument list; defaults to ``sys.argv`` when ``None``.

    Returns:
        The parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Generate batch event data and append it to a ClickHouse table."
        )
    )
    parser.add_argument(
        "-n", "--total-rows", type=int, required=True,
        help="Total number of rows to produce.",
    )
    for name in _MUTATION_FLAGS:
        parser.add_argument(
            f"--{name}", type=_int_or_all, default=0,
            help=f"Rows receiving the '{name}' mutation (an integer or ALL).",
        )
    parser.add_argument(
        "--table", required=True,
        help="Target table in 'database.table' form (database must exist).",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Rows per ClickHouse insert (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--ch-host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        help="ClickHouse host (env: CLICKHOUSE_HOST, default: localhost).",
    )
    parser.add_argument(
        "--ch-port", type=int,
        default=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        help="ClickHouse HTTP port (env: CLICKHOUSE_PORT, default: 8123).",
    )
    parser.add_argument(
        "--ch-user", default=os.environ.get("CLICKHOUSE_USER", "default"),
        help="ClickHouse user (env: CLICKHOUSE_USER, default: default).",
    )
    parser.add_argument(
        "--ch-password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        help="ClickHouse password (env: CLICKHOUSE_PASSWORD).",
    )
    parser.add_argument(
        "--state-file", type=Path, default=Path(".batch_wap_state.json"),
        help="Path to the id sequence state file.",
    )
    return parser.parse_args(argv)


def main(argv: Union[list[str], None] = None) -> None:
    """Generate rows, write them to ClickHouse and persist the id sequence.

    Args:
        argv: Optional argument list; defaults to ``sys.argv`` when ``None``.
    """
    args = _parse_args(argv)
    total = args.total_rows
    database, table = _split_table(args.table)

    mutations = MutationCounts(
        total=total,
        null_message=_resolve(args.null_message, total),
        duplicate_id=_resolve(args.duplicate_id, total),
        extra_user_id=_resolve(args.extra_user_id, total),
        missing_event_ts=_resolve(args.missing_event_ts, total),
        rename_event_type=_resolve(args.rename_event_type, total),
    )

    run_ts = datetime.now(timezone.utc)
    batch_id = uuid.uuid4().hex

    start_id = read_last_id(args.state_file)
    records = generate_records(mutations, start_id, run_ts)
    rows = pack_rows(records, batch_id, run_ts)

    sink = ClickHouseSink(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=database,
        table=table,
        batch_size=args.batch_size,
    )
    try:
        written = sink.write(rows)
    finally:
        sink.close()

    write_last_id(args.state_file, start_id + total)
    print(
        f"Wrote {written} rows to {args.table} "
        f"(batch_id={batch_id}, ids {start_id + 1}-{start_id + total})."
    )


if __name__ == "__main__":
    main()
