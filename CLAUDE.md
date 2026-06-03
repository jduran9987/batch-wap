# CLAUDE.md

Guidance for Claude when working in this repository.

## Project Overview

- This project is managed by `uv` and targets Python 3.12.

## Code Style

- Write a docstring for every module, class, and function.
- Annotate all function arguments and return values with types.
- Conform to ruff's default linting rules; fix all warnings before finishing.

## Executing Python Code

- Run every command through `uv` (e.g. `uv run`, `uv add`). Never invoke `python`, `pip`, or `pytest` directly.

## Project Description

`batch_wap` builds and compares **Write-Audit-Publish (WAP)** ingestion jobs.
Each job validates and promotes data from a mocked source into a production
table: rows that pass validation are flattened and published, rows that fail are
quarantined for inspection. The WAP guarantee is that nothing reaches the
production table until it has been audited.

The point of the project is to run these jobs under stress and learn how the
design must change as conditions get harder. We deliberately vary three axes:

- **Volume** — how much data each run must process.
- **Validation failures** — the rate and kinds of bad rows entering the pipeline.
- **Schema drift** — unexpected changes to the shape of incoming data (missing,
  extra, or renamed fields).

By turning these knobs and watching where validation, quarantine, and promotion
strain, we develop multiple ingestion-job versions and understand the tradeoffs
each makes.

There are three main components:

1. **`batch_data_generator`** — the mocked data source. It lets us control data
   volume, the validation errors produced, and the degree of schema drift.
2. **Ingestion jobs** — validate, flatten, quarantine, and promote the raw data
   produced by the source. Multiple versions exist to handle different scenarios.
3. **ClickHouse warehouse** — houses all the tables in the architecture.

## Mocked Data Source

The mocked source is the `batch_data_generator` package at
`src/batch_wap/sources/batch_data_generator/`. It produces controlled batches of
raw records with configurable volume, validation defects, and schema drift, so
the ingestion jobs can be exercised against realistic "bad" data.

The generator's output is **deterministic and contract-bound** — the ingestion
jobs depend on its keys, defect counts, and field values being stable, so those
invariants must be preserved when changing it. Before editing this package, read
its own `CLAUDE.md` ([`src/batch_wap/sources/batch_data_generator/CLAUDE.md`](src/batch_wap/sources/batch_data_generator/CLAUDE.md)),
which captures the load-bearing invariants and module ownership.

For the table schema, CLI usage, and the full list of options, see the package
README: [`src/batch_wap/sources/batch_data_generator/README.md`](src/batch_wap/sources/batch_data_generator/README.md).

## Ingestion

The ingestion layer reads raw data landed by the source and runs it through the
WAP cycle: **validate** incoming rows, **flatten** the ones that pass into the
production shape, **quarantine** the ones that fail, and **promote** the clean,
flattened rows into the production table.

Expect **multiple versions** of the ingestion jobs to live side by side (e.g.
`src/batch_wap/ingestion/wap_v1/`). Each version is a different approach to the
same problem, built to cope with a particular stress scenario — higher volume,
heavier validation-failure rates, or more aggressive schema drift. When adding
or editing a job, keep versions isolated rather than mutating a shared one in
place, so their behavior can be compared.

## Project Structure

```
.
├── config
│   └── users.d
│       └── data_lake.yaml
├── docker-compose.yaml
├── pyproject.toml
├── README.md
├── src
│   └── batch_wap
│       ├── __init__.py
│       ├── ingestion
│       │   └── wap_v1
│       └── sources
│           ├── __init__.py
│           └── batch_data_generator
│               ├── __init__.py
│               ├── cli.py
│               ├── envelope.py
│               ├── generator.py
│               ├── README.md
│               ├── sink.py
│               └── state.py
```
