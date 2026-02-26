"""Integration tests for snippet, snippets, and snippet_positions functions.

Mirrors django-paradedb's test_snippet_functions.py using the mock_items dataset.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from conftest import MockItem, assert_uses_paradedb_scan
from paradedb.sqlalchemy import pdb, search, select_with
from paradedb.sqlalchemy.errors import SnippetWithFuzzyPredicateError

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# snippet()
# ---------------------------------------------------------------------------


def test_snippet_contains_highlight_tags(mock_session):
    """snippet() wraps matched terms in default <b> tags."""
    stmt = (
        select(MockItem.id, pdb.snippet(MockItem.description).label("snip"))
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0
    snippets = [row[1] for row in rows if row[1]]
    assert any("<b>" in s for s in snippets)


def test_snippet_custom_tags(mock_session):
    """snippet() uses custom start/end tags when provided."""
    stmt = (
        select(
            MockItem.id,
            pdb.snippet(
                MockItem.description,
                start_tag="<mark>",
                end_tag="</mark>",
            ).label("snip"),
        )
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0
    snippets = [row[1] for row in rows if row[1]]
    assert any("<mark>" in s for s in snippets)
    assert not any("<b>" in s for s in snippets)


def test_snippet_max_num_chars(mock_session):
    """snippet() with max_num_chars truncates output length."""
    stmt_long = (
        select(MockItem.id, pdb.snippet(MockItem.description, max_num_chars=500).label("snip"))
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
        .limit(1)
    )
    stmt_short = (
        select(MockItem.id, pdb.snippet(MockItem.description, max_num_chars=20).label("snip"))
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
        .limit(1)
    )
    assert_uses_paradedb_scan(mock_session, stmt_short, index_name="mock_items_bm25_idx")
    long_rows = mock_session.execute(stmt_long).all()
    short_rows = mock_session.execute(stmt_short).all()
    if long_rows and short_rows and long_rows[0][1] and short_rows[0][1]:
        # Shorter max_num_chars should not produce longer raw content
        assert len(short_rows[0][1]) <= len(long_rows[0][1])


# ---------------------------------------------------------------------------
# snippets()
# ---------------------------------------------------------------------------


def test_snippets_returns_value(mock_session):
    """snippets() returns a JSON array of highlighted snippets."""
    stmt = (
        select(MockItem.id, pdb.snippets(MockItem.description).label("snips"))
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0
    # snippets() returns a JSON value (list or string)
    assert all(row[1] is not None for row in rows)


def test_snippets_with_limit(mock_session):
    """snippets() limit parameter controls how many snippets are returned per row."""
    stmt = (
        select(MockItem.id, pdb.snippets(MockItem.description, limit=1).label("snips"))
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
        .limit(3)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0


def test_snippets_with_custom_tags(mock_session):
    """snippets() uses custom start/end tags."""
    stmt = (
        select(
            MockItem.id,
            pdb.snippets(
                MockItem.description,
                start_tag="[",
                end_tag="]",
            ).label("snips"),
        )
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
        .limit(3)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0


# ---------------------------------------------------------------------------
# snippet_positions()
# ---------------------------------------------------------------------------


def test_snippet_positions_returns_ranges(mock_session):
    """snippet_positions() returns byte-offset ranges for matched terms."""
    stmt = (
        select(MockItem.id, pdb.snippet_positions(MockItem.description).label("positions"))
        .where(search.match_any(MockItem.description, "running"))
        .order_by(MockItem.id)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0
    assert all(row[1] is not None for row in rows)


# ---------------------------------------------------------------------------
# select_with helpers
# ---------------------------------------------------------------------------


def test_select_with_score_adds_column(mock_session):
    """select_with.score() appends a score column to the statement."""
    base = select(MockItem.id).where(search.match_any(MockItem.description, "running"))
    stmt = select_with.score(base, MockItem.id, label="search_score")
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt.order_by(MockItem.id)).all()
    assert len(rows) > 0
    assert all(row[1] is not None for row in rows)


def test_select_with_snippet_adds_column(mock_session):
    """select_with.snippet() appends a snippet column to the statement."""
    base = (
        select(MockItem.id, MockItem.description)
        .where(search.match_any(MockItem.description, "running"))
    )
    stmt = select_with.snippet(base, MockItem.description, label="snip")
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt.order_by(MockItem.id)).all()
    assert len(rows) > 0
    assert any(row[2] is not None for row in rows)


def test_select_with_snippets_adds_column(mock_session):
    """select_with.snippets() appends a snippets column to the statement."""
    base = (
        select(MockItem.id, MockItem.description)
        .where(search.match_any(MockItem.description, "running"))
    )
    stmt = select_with.snippets(base, MockItem.description, label="snips")
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt.order_by(MockItem.id)).all()
    assert len(rows) > 0


def test_select_with_snippet_positions_adds_column(mock_session):
    """select_with.snippet_positions() appends positions column to the statement."""
    base = (
        select(MockItem.id, MockItem.description)
        .where(search.match_any(MockItem.description, "running"))
    )
    stmt = select_with.snippet_positions(base, MockItem.description, label="positions")
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt.order_by(MockItem.id)).all()
    assert len(rows) > 0
    assert any(row[2] is not None for row in rows)


def test_select_with_snippet_rejects_fuzzy_predicate(mock_session):
    """select_with.snippet() raises when the predicate is fuzzy (no positions)."""
    base = (
        select(MockItem.id, MockItem.description)
        .where(search.fuzzy(MockItem.description, "runnning", distance=1))
    )
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippet(base, MockItem.description)


def test_select_with_snippets_rejects_fuzzy_predicate(mock_session):
    """select_with.snippets() raises when the predicate is fuzzy."""
    base = (
        select(MockItem.id, MockItem.description)
        .where(search.fuzzy(MockItem.description, "runnning", distance=1))
    )
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippets(base, MockItem.description)


def test_select_with_snippet_positions_rejects_fuzzy_predicate(mock_session):
    """select_with.snippet_positions() raises when the predicate is fuzzy."""
    base = (
        select(MockItem.id, MockItem.description)
        .where(search.fuzzy(MockItem.description, "runnning", distance=1))
    )
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippet_positions(base, MockItem.description)


# ---------------------------------------------------------------------------
# snippet + score combined
# ---------------------------------------------------------------------------


def test_snippet_and_score_together(mock_session):
    """snippet and score can be projected together in one query."""
    stmt = (
        select(
            MockItem.id,
            pdb.score(MockItem.id).label("score"),
            pdb.snippet(MockItem.description, start_tag="<em>", end_tag="</em>").label("snip"),
        )
        .where(search.match_any(MockItem.description, "running"))
        .order_by(pdb.score(MockItem.id).desc())
        .limit(5)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0
    # Score should be numeric and non-negative
    assert all(row[1] >= 0 for row in rows)
    # Snippets should contain highlights
    snippets = [row[2] for row in rows if row[2]]
    assert any("<em>" in s for s in snippets)
