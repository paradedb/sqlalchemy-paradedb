"""Integration tests for ParadeDB diagnostics helpers."""

from __future__ import annotations

import pytest
from sqlalchemy.engine import Engine

from paradedb.sqlalchemy.diagnostics import (
    paradedb_index_segments,
    paradedb_indexes,
    paradedb_verify_all_indexes,
    paradedb_verify_index,
)

pytestmark = pytest.mark.integration

# The index set up in conftest.py that these tests rely on.
_INDEX_NAME = "products_bm25_idx"

_REQUIRED_FUNCTIONS = {"indexes", "index_segments", "verify_index", "verify_all_indexes"}


@pytest.fixture(scope="module", autouse=True)
def require_diagnostics(engine: Engine) -> None:
    """Skip the entire module if this pg_search version lacks diagnostics."""
    with engine.connect() as conn:
        result = conn.exec_driver_sql(
            """
            SELECT DISTINCT p.proname
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'pdb'
              AND p.proname = ANY(%s)
            """,
            (list(_REQUIRED_FUNCTIONS),),
        )
        available = {row[0] for row in result.fetchall()}

    missing = sorted(_REQUIRED_FUNCTIONS - available)
    if missing:
        pytest.skip(
            "ParadeDB diagnostics not available in this pg_search version: "
            + ", ".join(missing)
        )


# ---------------------------------------------------------------------------
# paradedb_indexes
# ---------------------------------------------------------------------------


def test_paradedb_indexes_returns_list(engine: Engine) -> None:
    rows = paradedb_indexes(engine)
    assert isinstance(rows, list)


def test_paradedb_indexes_includes_products_index(engine: Engine) -> None:
    rows = paradedb_indexes(engine)
    names = [row["indexname"] for row in rows]
    assert _INDEX_NAME in names, f"Expected {_INDEX_NAME!r} in {names}"


def test_paradedb_indexes_rows_are_dicts(engine: Engine) -> None:
    rows = paradedb_indexes(engine)
    assert rows, "Expected at least one BM25 index"
    assert isinstance(rows[0], dict)


# ---------------------------------------------------------------------------
# paradedb_index_segments
# ---------------------------------------------------------------------------


def test_paradedb_index_segments_returns_list(engine: Engine) -> None:
    rows = paradedb_index_segments(engine, _INDEX_NAME)
    assert isinstance(rows, list)


def test_paradedb_index_segments_has_expected_keys(engine: Engine) -> None:
    rows = paradedb_index_segments(engine, _INDEX_NAME)
    assert len(rows) > 0, "Expected at least one segment"
    first = rows[0]
    assert "segment_idx" in first, f"Missing 'segment_idx' in {first}"
    assert "segment_id" in first, f"Missing 'segment_id' in {first}"


def test_paradedb_index_segments_invalid_index_raises(engine: Engine) -> None:
    with pytest.raises(Exception, match="does not exist|invalid|regclass"):
        paradedb_index_segments(engine, "nonexistent_index_xyz")


# ---------------------------------------------------------------------------
# paradedb_verify_index
# ---------------------------------------------------------------------------


def test_paradedb_verify_index_returns_list(engine: Engine) -> None:
    rows = paradedb_verify_index(engine, _INDEX_NAME)
    assert isinstance(rows, list)


def test_paradedb_verify_index_has_expected_keys(engine: Engine) -> None:
    rows = paradedb_verify_index(engine, _INDEX_NAME)
    assert len(rows) > 0, "Expected at least one verification result"
    first = rows[0]
    assert "check_name" in first, f"Missing 'check_name' in {first}"
    assert "passed" in first, f"Missing 'passed' in {first}"
    assert "details" in first, f"Missing 'details' in {first}"


def test_paradedb_verify_index_all_checks_pass(engine: Engine) -> None:
    rows = paradedb_verify_index(engine, _INDEX_NAME)
    failures = [r for r in rows if not r["passed"]]
    assert not failures, f"Verification failures: {failures}"


def test_paradedb_verify_index_with_sample_rate(engine: Engine) -> None:
    rows = paradedb_verify_index(engine, _INDEX_NAME, sample_rate=0.5)
    assert isinstance(rows, list)
    assert len(rows) > 0


def test_paradedb_verify_index_with_heapallindexed(engine: Engine) -> None:
    rows = paradedb_verify_index(engine, _INDEX_NAME, heapallindexed=True)
    assert isinstance(rows, list)
    assert len(rows) > 0


def test_paradedb_verify_index_invalid_index_raises(engine: Engine) -> None:
    with pytest.raises(Exception, match="does not exist|invalid|regclass"):
        paradedb_verify_index(engine, "nonexistent_index_xyz")


# ---------------------------------------------------------------------------
# paradedb_verify_all_indexes
# ---------------------------------------------------------------------------


def test_paradedb_verify_all_indexes_returns_list(engine: Engine) -> None:
    rows = paradedb_verify_all_indexes(engine)
    assert isinstance(rows, list)


def test_paradedb_verify_all_indexes_has_expected_keys(engine: Engine) -> None:
    rows = paradedb_verify_all_indexes(engine)
    assert len(rows) > 0, "Expected at least one result"
    first = rows[0]
    assert "check_name" in first
    assert "passed" in first


def test_paradedb_verify_all_indexes_filtered_by_index_pattern(engine: Engine) -> None:
    rows = paradedb_verify_all_indexes(engine, index_pattern=_INDEX_NAME)
    assert len(rows) > 0
    assert all(r["passed"] for r in rows), f"Unexpected failures: {rows}"


def test_paradedb_verify_all_indexes_no_match_pattern_returns_empty(
    engine: Engine,
) -> None:
    rows = paradedb_verify_all_indexes(engine, index_pattern="zzz_no_such_index_zzz")
    assert rows == []


def test_paradedb_verify_all_indexes_with_schema_pattern(engine: Engine) -> None:
    rows = paradedb_verify_all_indexes(engine, schema_pattern="public")
    assert isinstance(rows, list)


def test_paradedb_verify_all_indexes_with_sample_rate(engine: Engine) -> None:
    rows = paradedb_verify_all_indexes(engine, sample_rate=0.5)
    assert isinstance(rows, list)
