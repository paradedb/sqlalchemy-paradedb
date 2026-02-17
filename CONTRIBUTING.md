# Contributing

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[test,dev]
```

## Run Checks

```bash
ruff check .
mypy paradedb
python -m pytest tests/unit
PARADEDB_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres python -m pytest -m integration
```

## Guidelines

- Keep helpers typed and composable with standard SQLAlchemy expressions.
- Add integration tests for runtime behavior changes.
- Add unit tests for SQL compilation and validation paths.
- Preserve PostgreSQL-only safeguards for ParadeDB-specific expressions.
