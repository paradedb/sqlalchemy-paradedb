from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from conftest import MockItem, Product, assert_uses_paradedb_scan
from paradedb.sqlalchemy import facets, pdb, search


pytestmark = pytest.mark.integration


def _sql(stmt) -> str:
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    return "\n".join(line.rstrip() for line in sql.split("\n")).strip()


def test_agg_value_count_with_search_predicate(session):
    stmt = (
        select(pdb.agg(facets.value_count(field="id")))
        .select_from(Product)
        .where(search.match_all(Product.description, "running"))
    )
    assert_uses_paradedb_scan(session, stmt)
    assert (
        _sql(stmt)
        == """\
SELECT pdb.agg('{"value_count":{"field":"id"}}') AS agg_1
FROM products
WHERE products.description &&& 'running'"""
    )
    payload = session.execute(stmt).scalar_one()
    assert payload is not None


def test_multiple_agg_columns_with_search_all(session):
    stmt = (
        select(
            pdb.agg(facets.avg(field="rating")).label("avg_rating"),
            pdb.agg(facets.value_count(field="id")).label("count"),
        )
        .select_from(Product)
        .where(search.all(Product.id))
    )
    assert_uses_paradedb_scan(session, stmt)
    assert (
        _sql(stmt)
        == """\
SELECT pdb.agg('{"avg":{"field":"rating"}}') AS avg_rating, pdb.agg('{"value_count":{"field":"id"}}') AS count
FROM products
WHERE products.id @@@ pdb.all()"""
    )
    row = session.execute(stmt).one()
    assert row._mapping["avg_rating"] is not None
    assert row._mapping["count"] is not None


def test_percentiles_agg_with_search_all(session):
    stmt = (
        select(pdb.agg(facets.percentiles(field="rating", percents=[50, 95])).label("pct"))
        .select_from(Product)
        .where(search.all(Product.id))
    )
    assert_uses_paradedb_scan(session, stmt)
    assert (
        _sql(stmt)
        == """\
SELECT pdb.agg('{"percentiles":{"field":"rating","percents":[50,95]}}') AS pct
FROM products
WHERE products.id @@@ pdb.all()"""
    )
    row = session.execute(stmt).one()
    assert row._mapping["pct"] is not None


def test_top_hits_agg_with_search_all(session):
    stmt = (
        select(
            pdb.agg(
                facets.top_hits(
                    size=2,
                    sort=[{"rating": "desc"}],
                    docvalue_fields=["id", "rating"],
                )
            ).label("hits")
        )
        .select_from(Product)
        .where(search.all(Product.id))
    )
    assert_uses_paradedb_scan(session, stmt)
    assert (
        _sql(stmt)
        == """\
SELECT pdb.agg('{"top_hits":{"docvalue_fields":["id","rating"],"size":2,"sort":[{"rating":"desc"}]}}') AS hits
FROM products
WHERE products.id @@@ pdb.all()"""
    )
    row = session.execute(stmt).one()
    assert row._mapping["hits"] is not None


def test_window_agg_with_raw_query_operators(mock_session):
    base = (
        select(MockItem.id, MockItem.description, MockItem.rating)
        .where(
            search.all(MockItem.id),
            search.term(MockItem.category, "electronics"),
        )
        .order_by(MockItem.rating.desc())
        .limit(3)
    )

    stmt = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=MockItem.id)

    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    assert (
        _sql(stmt)
        == """\
SELECT mock_items.id, mock_items.description, mock_items.rating, pdb.agg('{"value_count":{"field":"id"}}') OVER () AS facets
FROM mock_items
WHERE mock_items.id @@@ pdb.all() AND mock_items.category === 'electronics' ORDER BY mock_items.rating DESC
 LIMIT 3"""
    )
    rows = mock_session.execute(stmt).all()
    assert len(rows) == 3
    assert [row.rating for row in rows] == [5, 4, 4]
    assert {row.id for row in rows[0:]} == {12, 1, 2}
    assert facets.extract(rows) == {"value": 5.0}
    assert all(row.facets == {"value": 5.0} for row in rows)


def test_with_rows_adds_window_agg_and_extracts_payload(session):
    base = (
        select(Product.id, Product.description, Product.rating)
        .where(Product.rating >= 4)
        .order_by(Product.rating.desc())
        .limit(3)
    )
    stmt = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=Product.id)
    assert_uses_paradedb_scan(session, stmt)

    assert (
        _sql(stmt)
        == """\
SELECT products.id, products.description, products.rating, pdb.agg('{"value_count":{"field":"id"}}') OVER () AS facets
FROM products
WHERE products.rating >= 4 AND products.id @@@ pdb.all() ORDER BY products.rating DESC
 LIMIT 3"""
    )
    rows = session.execute(stmt).all()
    assert len(rows) == 3
    assert {row.id for row in rows} == {1, 2, 3}
    assert facets.extract(rows) == {"value": 3.0}


def test_with_rows_auto_injects_sentinel_when_no_paradedb_predicate(session):
    base = (
        select(Product.id, Product.description, Product.rating)
        .where(Product.rating >= 4)
        .order_by(Product.rating.desc())
        .limit(3)
    )
    stmt = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=Product.id)
    assert (
        _sql(stmt)
        == """\
SELECT products.id, products.description, products.rating, pdb.agg('{"value_count":{"field":"id"}}') OVER () AS facets
FROM products
WHERE products.rating >= 4 AND products.id @@@ pdb.all() ORDER BY products.rating DESC
 LIMIT 3"""
    )
    assert_uses_paradedb_scan(session, stmt)
    rows = session.execute(stmt).all()
    assert facets.extract(rows) == {"value": 3.0}
