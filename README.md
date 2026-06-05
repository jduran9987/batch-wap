# Write Audit Publish Patterns for Batch Processing

A write-audit-publish (WAP) data pipeline built around an Apache Iceberg
lakehouse on AWS (Glue catalog + S3). The project produces batches of event
data, lands them in Iceberg, and supports auditing and querying the results
before they are promoted downstream.

## Batch Data Generator

`src/batch_wap/sources/batch_data_generator/` produces batches of synthetic,
**unstructured** event records and appends them to an Iceberg table backed by an
AWS Glue catalog and an S3 warehouse. Each batch is tagged with its own id and
load timestamp so runs can be grouped and ordered after the fact.

The generator can deliberately introduce data-quality defects — null values,
duplicate ids, missing fields, extra fields, and renamed fields — in a
configurable number of rows, which makes it useful for exercising the audit and
validation stages of the pipeline.

See the package README for the table schema, CLI usage, and the full list of
options: [`src/batch_wap/sources/batch_data_generator/README.md`](src/batch_wap/sources/batch_data_generator/README.md).

## ClickHouse

All tables produced by this application will be written to ClickHouse, which is deployed as a container using Docker Compose.

Start it:

```bash
docker compose up -d
```

Then open the Play UI in the browser: <http://localhost:8123/play>

### Query a table

Reference tables as `<database.table>`:

```sql
SELECT *
FROM raw.raw_events
LIMIT 10;
```
