# Testing

## Unit Tests

Run the tests to verify every change:

```bash
.venv/bin/python -m pytest -m "not integration"
scripts/run_integration_tests.sh
```

To run a subset of tests, pass pytest selectors:

```bash
scripts/run_integration_tests.sh tests/integration/test_indexing_integration.py::test_bm25_partial_index_generates_where_clause
```

The script sets `PARADEDB_TEST_DSN` and `DATABASE_URL` automatically. The default container name is `paradedb-sqlalchemy-integration` on port `5443`.

Some integration tests require newer pg_search versions and will be automatically skipped if the feature is not available (e.g. diagnostics functions like `pdb.indexes()`).
