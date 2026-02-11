from __future__ import annotations

import pytest
from sqlalchemy import select

from paradedb.sqlalchemy import pdb, search, select_with
from conftest import Product


pytestmark = pytest.mark.integration


def test_match_all_returns_expected_rows(session):
    stmt = select(Product.id).where(search.match_all(Product.description, "running", "shoes")).order_by(Product.id)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_phrase_match_returns_expected_row(session):
    stmt = select(Product.id).where(search.phrase(Product.description, "running shoes")).order_by(Product.id)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_term_exact_token_match(session):
    stmt = select(Product.id).where(search.term(Product.description, "wireless"))
    ids = list(session.scalars(stmt))
    assert ids == [3]


def test_regex_match(session):
    stmt = select(Product.id).where(search.regex(Product.description, "run.*")).order_by(Product.id)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_fuzzy_match(session):
    stmt = select(Product.id).where(search.fuzzy(Product.description, "wirless", distance=1))
    ids = list(session.scalars(stmt))
    assert ids == [3]


def test_score_and_ordering(session):
    stmt = (
        select(Product.id, pdb.score(Product.id).label("score"))
        .where(search.match_all(Product.description, "running", "shoes"))
        .order_by(pdb.score(Product.id).desc(), Product.id.asc())
    )
    rows = session.execute(stmt).all()

    assert [row[0] for row in rows] == [1, 2]
    assert rows[0][1] >= rows[1][1]


def test_snippet_projection(session):
    stmt = (
        select(Product.id, pdb.snippet(Product.description, start_tag="<mark>", end_tag="</mark>").label("snippet"))
        .where(search.match_any(Product.description, "running"))
        .order_by(Product.id)
    )
    rows = session.execute(stmt).all()

    assert [row[0] for row in rows] == [1, 2]
    assert "<mark>" in rows[0][1]


def test_select_with_helpers(session):
    base = select(Product.id).where(search.match_any(Product.description, "running"))
    stmt = select_with.score(base, Product.id, label="search_score")

    rows = session.execute(stmt.order_by(Product.id)).all()
    assert len(rows) == 2
    assert rows[0][1] is not None
