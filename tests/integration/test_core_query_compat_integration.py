from __future__ import annotations

import pytest
from sqlalchemy import and_, not_, or_, select
from sqlalchemy.orm import aliased

from conftest import Product, assert_uses_paradedb_scan
from paradedb.sqlalchemy import search


pytestmark = pytest.mark.integration


def test_search_with_aliased_entity(session):
    product_alias = aliased(Product)
    stmt = select(product_alias.id).where(search.term(product_alias.description, "wireless"))
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [3]


def test_search_in_cte_then_structured_outer_filter(session):
    matched = select(Product.id.label("pid")).where(search.match_any(Product.description, "running")).cte("matched")

    stmt = (
        select(Product.id).where(Product.id.in_(select(matched.c.pid))).where(Product.rating >= 5).order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_search_with_subquery_columns(session):
    sq = select(
        Product.id.label("pid"),
        Product.description.label("description"),
    ).subquery()
    stmt = select(sq.c.pid).where(search.match_all(sq.c.description, "running", "shoes")).order_by(sq.c.pid)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_search_with_cte_columns(session):
    base = select(
        Product.id.label("pid"),
        Product.description.label("description"),
    ).cte("base")
    stmt = select(base.c.pid).where(search.match_any(base.c.description, "wireless")).order_by(base.c.pid)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [3]


def test_boolean_composition_with_search_predicates(session):
    stmt = (
        select(Product.id)
        .where(
            or_(
                and_(
                    search.match_all(Product.description, "running"),
                    not_(search.match_any(Product.description, "trail")),
                ),
                and_(
                    search.match_any(Product.category, "Electronics"),
                    search.match_any(Product.description, "wireless"),
                ),
            )
        )
        .order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 3]
