"""Iceberg sink backed by an AWS Glue catalog and an S3 warehouse.

Loads the catalog, creates the target table with a fixed four-column envelope
schema when it does not yet exist, and appends the packed rows. Because event
payloads are stored as an opaque JSON string, the table schema never changes,
so no schema evolution is required. The namespace/database is assumed to
already exist; only the table is created.
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow

TABLE_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("batch_id", pa.string(), nullable=True),
        pa.field("data", pa.string(), nullable=True),
        pa.field("hashed_json", pa.string(), nullable=True),
        pa.field("loaded_at", pa.timestamp("us", tz="UTC"), nullable=True),
    ]
)


class IcebergSink:
    """Creates the envelope table if needed and appends rows to it."""

    def __init__(self, catalog_name: str, warehouse: str, identifier: str) -> None:
        """Initialise the sink and load the Glue catalog.

        Args:
            catalog_name: Name of the Glue catalog to load.
            warehouse: S3 warehouse URI for the catalog.
            identifier: Fully qualified ``namespace.table`` identifier. The
                namespace must already exist.
        """
        self._identifier = identifier
        self._catalog: Catalog = load_catalog(
            catalog_name, **{"type": "glue", "warehouse": warehouse}
        )

    def write(self, data: pa.Table) -> None:
        """Create the table if absent, then append ``data`` to it.

        Args:
            data: The packed envelope rows to append.
        """
        try:
            table = self._catalog.load_table(self._identifier)
        except NoSuchTableError:
            table = self._catalog.create_table(self._identifier, schema=TABLE_SCHEMA)
        table.append(data.cast(schema_to_pyarrow(table.schema())))
