from __future__ import annotations

import pytest
from sqlalchemy import select

from conftest import Product, assert_uses_paradedb_scan
from paradedb.sqlalchemy import search


pytestmark = pytest.mark.integration


def test_parse_query_builder_predicate(session):
    stmt = select(Product.id).where(search.parse(Product.id, "description:sleek", lenient=True)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_phrase_prefix_predicate(session):
    stmt = select(Product.id).where(search.phrase_prefix(Product.description, ["running", "sh"])).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_regex_phrase_predicate(session):
    stmt = select(Product.id).where(search.regex_phrase(Product.description, ["run.*", "shoe.*"], slop=1)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_near_predicate(session):
    stmt = select(Product.id).where(search.near(Product.description, "sleek", "shoes", distance=3)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_proximity_with_prox_array_and_regex(session):
    prox = search.prox_array(search.prox_regex("sl.*"), "running").near("shoes", distance=1)
    stmt = select(Product.id).where(search.proximity(Product.description, prox)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert 1 in ids


def test_more_like_this_by_document_id(session):
    stmt = select(Product.id).where(search.more_like_this(Product.id, document_id=1, fields=["description"])).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert 2 in ids


def test_more_like_this_by_document_payload(session):
    stmt = select(Product.id).where(
        search.more_like_this(Product.id, document={"description": "wireless noise-canceling headphones"})
    )
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert 3 in ids
