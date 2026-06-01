"""Command-line interface for the batch event-data generator.

Parses arguments, generates unstructured event records (with any requested
mutations), packs them into the Iceberg envelope and appends them to the target
table, then advances the persisted id sequence. The mutation flags accept
either an integer or the literal ``ALL``, where ``ALL`` resolves to the total
row count.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

from batch_wap.sources.batch_data_generator.envelope import pack
from batch_wap.sources.batch_data_generator.generator import (
    MutationCounts,
    generate_records,
)
from batch_wap.sources.batch_data_generator.sink import IcebergSink
from batch_wap.sources.batch_data_generator.state import read_last_id, write_last_id

_MUTATION_FLAGS: list[str] = [
    "null-message",
    "duplicate-id",
    "extra-user-id",
    "missing-event-ts",
    "rename-event-type",
]


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


def _parse_args(argv: Union[list[str], None] = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Optional argument list; defaults to ``sys.argv`` when ``None``.

    Returns:
        The parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Generate batch event data and append it to an Iceberg table."
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
        help="Fully qualified namespace.table identifier (namespace must exist).",
    )
    parser.add_argument("--warehouse", required=True, help="S3 warehouse URI.")
    parser.add_argument("--catalog-name", default="glue", help="Glue catalog name.")
    parser.add_argument(
        "--state-file", type=Path, default=Path(".batch_wap_state.json"),
        help="Path to the id sequence state file.",
    )
    return parser.parse_args(argv)


def main(argv: Union[list[str], None] = None) -> None:
    """Generate rows, write them to Iceberg and persist the id sequence.

    Args:
        argv: Optional argument list; defaults to ``sys.argv`` when ``None``.
    """
    args = _parse_args(argv)
    total = args.total_rows

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
    table = pack(records, batch_id, run_ts)

    sink = IcebergSink(args.catalog_name, args.warehouse, args.table)
    sink.write(table)

    write_last_id(args.state_file, start_id + total)
    print(
        f"Wrote {table.num_rows} rows to {args.table} "
        f"(batch_id={batch_id}, ids {start_id + 1}-{start_id + total})."
    )


if __name__ == "__main__":
    main()
