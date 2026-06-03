# Contract tests — `batch_data_generator`

A small contract suite that pins the **observable output of the data generator**
so refactors can be proven not to have changed it.

These tests are intentionally **sink-agnostic** — they consume the generator's
rows in memory and never import `sink.py`. The storage backend (ClickHouse) is
the thing most likely to change, so it is deliberately excluded; what is
asserted is the data itself (keys, mutation counts, envelope shape) and the
state machine.

## Layout

```
tests/batch_data_generator/
├── _contract_api.py              # the ONLY file with API/record-shape assumptions
├── test_generation_contract.py   # counts, keys, mutations, envelope shape
├── test_state_contract.py        # id state file round-trip
└── README.md
```

There is intentionally no `__init__.py` here, so the directory stays on
`sys.path` and the test modules can `import _contract_api`.

## Running

All project commands run through `uv`. From the repo root:

```bash
uv sync
uv run pytest tests/batch_data_generator
```

## Adjusting to the real code

Everything that depends on the package's actual API or record shape is isolated
in `_contract_api.py`:

- **Public API imports** — `generate_records`, `MutationCounts`, `pack_rows`,
  `read_last_id`, `write_last_id`.
- **Record field names** — `id`, `event_type` / `type` (renamed), `event_ts`
  (missing), `message` (nulled), `user_id` (extra).

If a refactor renames any of these, update `_contract_api.py` and the test
modules should keep working.
