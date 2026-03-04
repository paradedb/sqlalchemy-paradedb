# Contributing

## Setup

```bash
# Install uv: https://docs.astral.sh/uv/getting-started/installation/
uv sync --extra test --extra dev
```

## Run Checks

```bash
uv run --extra dev ruff check .
uv run --extra dev mypy paradedb
uv run --extra test pytest tests/unit
PARADEDB_TEST_DSN=postgresql+psycopg://postgres:postgres@localhost:5443/postgres uv run --extra test pytest -m integration
```

To catch issues before commit or push, install the local git hooks once:

```bash
uv run --extra dev pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push
```

## Guidelines

- Keep helpers typed and composable with standard SQLAlchemy expressions.
- Add integration tests for runtime behavior changes.
- Add unit tests for SQL compilation and validation paths.
- Preserve PostgreSQL-only safeguards for ParadeDB-specific expressions.
