# Write Audit Publish Patterns for Batch Processing

A write-audit-publish (WAP) data pipeline built around an Apache Iceberg
lakehouse on AWS (Glue catalog + S3). The project produces batches of event
data, lands them in Iceberg, and supports auditing and querying the results
before they are promoted downstream.

This README is a living document and will grow as the project develops.

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

ClickHouse queries the Iceberg lakehouse through the same Glue catalog the
generator writes to. It runs locally as a container.

Start it:

```bash
docker compose up -d
```

Then open the Play UI in the browser: <http://localhost:8123/play>

### Connect to the Glue catalog

Create a database backed by the Glue data lake catalog, supplying the AWS
credentials and region:

```sql
CREATE DATABASE datalake
ENGINE = DataLakeCatalog
SETTINGS
    catalog_type = 'glue',
    region = 'us-east-1',
    aws_access_key_id = '<AWS_ACCESS_KEY_ID>',
    aws_secret_access_key = '<AWS_SECRET_ACCESS_KEY>';
```

### Query a table

Reference tables as `<database>.<namespace.table>`:

```sql
SELECT *
FROM datalake.`batch_wap.raw_events`
LIMIT 10;
```
