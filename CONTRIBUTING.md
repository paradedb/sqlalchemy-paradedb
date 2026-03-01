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
PARADEDB_TEST_DSN=postgresql+psycopg://postgres:postgres@localhost:5443/postgres python -m pytest -m integration
```

To catch issues before commit or push, install the local git hooks once:

```bash
python -m pip install .[test,dev]
pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push
```

## Guidelines

- Keep helpers typed and composable with standard SQLAlchemy expressions.
- Add integration tests for runtime behavior changes.
- Add unit tests for SQL compilation and validation paths.
- Preserve PostgreSQL-only safeguards for ParadeDB-specific expressions.
