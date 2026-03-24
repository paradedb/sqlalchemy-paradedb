from __future__ import annotations

import pytest
from sqlalchemy import func, literal_column, select
from sqlalchemy.dialects import postgresql

from conftest import MockItem, Product, assert_uses_paradedb_scan
from paradedb.sqlalchemy import facets, pdb, search


pytestmark = pytest.mark.integration


def _sql(stmt) -> str:
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def test_agg_value_count_with_search_predicate(session):
    stmt = (
        select(pdb.agg(facets.value_count(field="id")))
        .select_from(Product)
        .where(search.match_all(Product.description, "running"))
    )
    assert_uses_paradedb_scan(session, stmt)
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
    row = session.execute(stmt).one()
    assert row._mapping["hits"] is not None


def test_window_agg_with_raw_query_operators(mock_session):
    base = (
        select(MockItem.id, MockItem.description, MockItem.rating)
        .where(
            MockItem.id.op("@@@")(func.pdb.all()),
            MockItem.category.op("===")(literal_column("'electronics'")),
        )
        .order_by(MockItem.rating.desc())
        .limit(3)
    )

    stmt, plan = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=MockItem.id)

    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    sql = " ".join(_sql(stmt).split())
    expected_sql = """
        SELECT mock_items.id, mock_items.description, mock_items.rating,
               pdb.agg('{"value_count":{"field":"id"}}') OVER () AS facets
        FROM mock_items
        WHERE (mock_items.id @@@ pdb.all()) AND (mock_items.category === 'electronics')
        ORDER BY mock_items.rating DESC
        LIMIT 3
    """
    assert sql == " ".join(expected_sql.split())
    assert plan.label == "facets"

    rows = mock_session.execute(stmt).all()
    assert len(rows) == 3
    assert [row.rating for row in rows] == [5, 4, 4]
    assert {row.id for row in rows[0:]} == {12, 1, 2}
    assert all(row.facets == {"value": 5.0} for row in rows)


def test_with_rows_adds_window_agg_and_extracts_payload(session):
    base = (
        select(Product.id, Product.description, Product.rating)
        .where(Product.rating >= 4)
        .order_by(Product.rating.desc())
        .limit(3)
    )
    stmt, facet_plan = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=Product.id)
    assert_uses_paradedb_scan(session, stmt)

    sql = _sql(stmt)
    assert "OVER () AS facets" in sql
    assert facet_plan.label == "facets"


def test_with_rows_auto_injects_sentinel_when_no_paradedb_predicate(session):
    base = (
        select(Product.id, Product.description, Product.rating)
        .where(Product.rating >= 4)
        .order_by(Product.rating.desc())
        .limit(3)
    )
    stmt, _ = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=Product.id)
    sql = _sql(stmt)
    assert "products.id @@@ pdb.all()" in sql
    assert_uses_paradedb_scan(session, stmt)
