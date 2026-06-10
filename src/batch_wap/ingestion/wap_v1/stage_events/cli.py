"""Command-line entrypoint for the WAP v1 staging job.

This module is intentionally thin: it parses runtime configuration, creates the
production ClickHouse reader/writer, and delegates the actual staging-job logic
to :func:`job.run`.

Airflow or another orchestrator can call this module as a subprocess and pass
the processing window explicitly, for example::

    uv run wap_v1_stage_events \
      --window-start "2026-06-05T12:00:00+00:00" \
      --window-end "2026-06-05T12:10:00+00:00" \
      --source-table raw.raw_events \
      --stg-table raw.stg_events \
      --quarantine-table quarantine.raw_events \
      --statistics-table statistics.stg_events
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Union

import clickhouse_connect

from batch_wap.ingestion.wap_v1.stage_events.clickhouse import ClickHouseReader, ClickHouseWriter
from batch_wap.ingestion.wap_v1.stage_events.job import run


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string and normalize it to UTC.

    Args:
        value: Datetime string, such as ``"2026-06-05T12:00:00+00:00"``.

    Returns:
        A timezone-aware UTC datetime.

    Raises:
        argparse.ArgumentTypeError: If the value is not a valid timezone-aware
            datetime.
    """
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime: {value!r}. Expected ISO-8601 format."
        ) from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            f"Datetime must be timezone-aware: {value!r}."
        )

    return parsed.astimezone(timezone.utc)


def _parse_table_name(value: str) -> tuple[str, str]:
    """Parse a fully qualified ClickHouse table name.

    Args:
        value: Table name in ``database.table`` form.

    Returns:
        A two-tuple of ``(database, table)``.

    Raises:
        argparse.ArgumentTypeError: If the value is not in ``database.table``
            form.
    """
    parts = value.split(".")
    if len(parts) != 2 or not all(parts):
        raise argparse.ArgumentTypeError(
            f"Invalid table name: {value!r}. Expected 'database.table'."
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
        description="Run the WAP v1 staging job for a ClickHouse raw-event window."
    )
    parser.add_argument(
        "--window-start",
        type=_parse_datetime,
        required=True,
        help="Inclusive window start as a timezone-aware ISO-8601 datetime.",
    )
    parser.add_argument(
        "--window-end",
        type=_parse_datetime,
        required=True,
        help="Exclusive window end as a timezone-aware ISO-8601 datetime.",
    )
    parser.add_argument(
        "--source-table",
        type=_parse_table_name,
        default=_parse_table_name(os.environ.get("WAP_SOURCE_TABLE", "raw.raw_events")),
        help=(
            "Source table in 'database.table' form "
            "(env: WAP_SOURCE_TABLE, default: raw.raw_events)."
        ),
    )
    parser.add_argument(
        "--stg-table",
        type=_parse_table_name,
        default=_parse_table_name(os.environ.get("WAP_STG_TABLE", "raw.stg_events")),
        help=(
            "Staging table in 'database.table' form "
            "(env: WAP_STG_TABLE, default: raw.stg_events)."
        ),
    )
    parser.add_argument(
        "--quarantine-table",
        type=_parse_table_name,
        default=_parse_table_name(
            os.environ.get("WAP_QUARANTINE_TABLE", "quarantine.raw_events")
        ),
        help=(
            "Quarantine table in 'database.table' form "
            "(env: WAP_QUARANTINE_TABLE, default: quarantine.raw_events)."
        ),
    )
    parser.add_argument(
        "--statistics-table",
        type=_parse_table_name,
        default=_parse_table_name(
            os.environ.get("WAP_STATISTICS_TABLE", "statistics.stg_events")
        ),
        help=(
            "Statistics table in 'database.table' form "
            "(env: WAP_STATISTICS_TABLE, default: statistics.stg_events)."
        ),
    )
    parser.add_argument(
        "--ch-host",
        default=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        help="ClickHouse host (env: CLICKHOUSE_HOST, default: localhost).",
    )
    parser.add_argument(
        "--ch-port",
        type=int,
        default=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        help="ClickHouse HTTP port (env: CLICKHOUSE_PORT, default: 8123).",
    )
    parser.add_argument(
        "--ch-user",
        default=os.environ.get("CLICKHOUSE_USER", "default"),
        help="ClickHouse user (env: CLICKHOUSE_USER, default: default).",
    )
    parser.add_argument(
        "--ch-password",
        default=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        help="ClickHouse password (env: CLICKHOUSE_PASSWORD).",
    )
    return parser.parse_args(argv)


def main(argv: Union[list[str], None] = None) -> None:
    """Run the WAP v1 staging job from command-line arguments.

    Args:
        argv: Optional argument list; defaults to ``sys.argv`` when ``None``.
    """
    args = _parse_args(argv)

    source_table = args.source_table
    stg_table = args.stg_table
    quarantine_table = args.quarantine_table
    statistics_table = args.statistics_table

    client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
    )

    reader = ClickHouseReader(
        client=client,
        database=source_database,
        table=source_table,
    )
    writer = ClickHouseWriter(
        client=client,
        stg_database=stg_database,
        stg_table=stg_table,
        quarantine_database=quarantine_database,
        quarantine_table=quarantine_table,
        statistics_database=statistics_database,
        statistics_table=statistics_table,
    )

    run(
        window_start=args.window_start,
        window_end=args.window_end,
        reader=reader,
        writer=writer,
    )


if __name__ == "__main__":
    main()
