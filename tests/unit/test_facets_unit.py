from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, Text, column, select, table
from sqlalchemy.dialects import postgresql

from paradedb.sqlalchemy import facets, pdb
from paradedb.sqlalchemy.errors import FacetRequiresLimitError, FacetRequiresOrderByError, InvalidArgumentError


products = table(
    "products",
    column("id", Integer),
    column("description", Text),
    column("category", String),
    column("rating", Integer),
)


def _sql(stmt) -> str:
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def test_metric_and_bucket_builders():
    assert facets.value_count(field="id") == {"value_count": {"field": "id"}}
    assert facets.avg(field="rating") == {"avg": {"field": "rating"}}
    assert facets.sum(field="rating") == {"sum": {"field": "rating"}}
    assert facets.min(field="rating") == {"min": {"field": "rating"}}
    assert facets.max(field="rating") == {"max": {"field": "rating"}}
    assert facets.stats(field="rating") == {"stats": {"field": "rating"}}
    assert facets.percentiles(field="rating", percents=[50, 95]) == {
        "percentiles": {"field": "rating", "percents": [50, 95]}
    }
    assert facets.histogram(field="rating", interval=1) == {"histogram": {"field": "rating", "interval": 1}}
    assert facets.date_histogram(field="created_at", fixed_interval="1d") == {
        "date_histogram": {"field": "created_at", "fixed_interval": "1d"}
    }
    assert facets.range(field="rating", ranges=[{"to": 3.0}, {"from": 3.0, "to": 5.0}]) == {
        "range": {"field": "rating", "ranges": [{"to": 3.0}, {"from": 3.0, "to": 5.0}]}
    }
    assert facets.top_hits(size=3, sort=[{"created_at": "desc"}], docvalue_fields=["id", "created_at"]) == {
        "top_hits": {"size": 3, "sort": [{"created_at": "desc"}], "docvalue_fields": ["id", "created_at"]}
    }
    assert facets.top_hits(size=3, from_=5) == {"top_hits": {"size": 3, "from": 5}}


def test_multi_merges_specs():
    spec = facets.multi(
        facets.avg(field="rating"), facets.value_count(field="id"), facets.terms(field="category", size=10)
    )
    assert spec == {
        "avg": {"field": "rating"},
        "value_count": {"field": "id"},
        "terms": {"field": "category", "size": 10},
    }


def test_with_rows_requires_order_and_limit():
    base_missing_order = select(products.c.id).limit(5)
    with pytest.raises(FacetRequiresOrderByError, match="requires ORDER BY"):
        facets.with_rows(base_missing_order, agg=facets.terms(field="category", size=10), key_field=products.c.id)

    base_missing_limit = select(products.c.id).order_by(products.c.id)
    with pytest.raises(FacetRequiresLimitError, match="requires LIMIT"):
        facets.with_rows(base_missing_limit, agg=facets.terms(field="category", size=10), key_field=products.c.id)


def test_with_rows_adds_window_agg_column():
    base = select(products.c.id, products.c.description).order_by(products.c.id).limit(10)
    stmt, facet_plan = facets.with_rows(base, agg=facets.terms(field="category", size=10), key_field=products.c.id)
    sql = _sql(stmt)

    assert 'pdb.agg(\'{"terms":{"field":"category","size":10}}\')' in sql
    assert "OVER () AS facets" in sql
    assert "products.id @@@ pdb.all()" in sql
    assert facet_plan.label == "facets"


def test_with_rows_accepts_fetch_clause():
    base = select(products.c.id, products.c.description).order_by(products.c.id).fetch(10)
    stmt, facet_plan = facets.with_rows(base, agg=facets.terms(field="category", size=10), key_field=products.c.id)
    sql = _sql(stmt)

    assert "OVER () AS facets" in sql
    assert "FETCH FIRST (10) ROWS ONLY" in sql
    assert "products.id @@@ pdb.all()" in sql
    assert facet_plan.label == "facets"


def test_facet_plan_extract_empty_rows_returns_none():
    base = select(products.c.id).order_by(products.c.id).limit(1)
    _, facet_plan = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=products.c.id)
    assert facet_plan.extract([]) is None


def test_percentiles_requires_non_empty_percents():
    with pytest.raises(InvalidArgumentError, match="percents must contain at least one value"):
        facets.percentiles(field="rating", percents=[])


def test_agg_approximate_true_generates_positional_false():
    # approximate=True → skip visibility checks → pass false as second positional arg
    stmt = select(pdb.agg(facets.value_count(field="id"), approximate=True))
    sql = _sql(stmt)
    assert "pdb.agg" in sql
    assert "approximate =>" not in sql  # must NOT use named-arg form
    assert "false" in sql.lower()


def test_agg_approximate_false_generates_positional_true():
    # approximate=False → force exact → pass true as second positional arg
    stmt = select(pdb.agg(facets.value_count(field="id"), approximate=False))
    sql = _sql(stmt)
    assert "pdb.agg" in sql
    assert "approximate =>" not in sql
    assert "true" in sql.lower()


def test_agg_no_approximate_omits_second_arg():
    stmt = select(pdb.agg(facets.value_count(field="id")))
    sql = _sql(stmt)
    assert 'pdb.agg(\'{"value_count":{"field":"id"}}\')' in sql
    # Only one argument — no trailing comma with a second value
    assert sql.count("pdb.agg(") == 1
