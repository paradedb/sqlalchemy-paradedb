from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, Text, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import column, table

from paradedb.sqlalchemy import pdb, search, select_with


products = table(
    "products",
    column("id", Integer),
    column("description", Text),
    column("category", String),
)


def _sql(stmt) -> str:
    return str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_match_all_multiple_terms_compile():
    stmt = select(products.c.id).where(search.match_all(products.c.description, "running", "shoes"))
    sql = _sql(stmt)
    assert "description &&& ARRAY['running', 'shoes']" in sql


def test_phrase_with_slop_and_boost_compile():
    stmt = select(products.c.id).where(search.phrase(products.c.description, "running shoes", slop=2, boost=3))
    sql = _sql(stmt)
    assert "description ###" in sql
    assert "::pdb.slop(2)::pdb.boost(3)" in sql


def test_fuzzy_compile_with_options():
    stmt = select(products.c.id).where(
        search.fuzzy(products.c.description, "shose", distance=1, prefix=False, transpose_cost_one=True)
    )
    sql = _sql(stmt)
    assert "description === 'shose'::pdb.fuzzy(1, f, t)" in sql


def test_regex_and_all_compile():
    regex_stmt = select(products.c.id).where(search.regex(products.c.description, "run.*"))
    all_stmt = select(products.c.id).where(search.all(products.c.id))
    regex_sql = _sql(regex_stmt)
    all_sql = _sql(all_stmt)

    assert "description @@@ pdb.regex('run.*')" in regex_sql
    assert "id @@@ pdb.all()" in all_sql


def test_pdb_helpers_compile():
    stmt = select(
        pdb.score(products.c.id).label("score"),
        pdb.snippet(products.c.description, start_tag="<mark>", end_tag="</mark>", max_num_chars=100).label("snippet"),
        pdb.snippets(products.c.description, max_num_chars=15, limit=1, offset=0, sort_by="position").label("snippets"),
        pdb.snippet_positions(products.c.description).label("positions"),
    )
    sql = _sql(stmt)

    assert "pdb.score(products.id) AS score" in sql
    assert "pdb.snippet(products.description, '<mark>', '</mark>', 100) AS snippet" in sql
    assert "pdb.snippets(products.description" in sql
    assert "max_num_chars => 15" in sql
    assert '"limit" => 1' in sql
    assert '"offset" => 0' in sql
    assert "sort_by => 'position'" in sql
    assert "pdb.snippet_positions(products.description) AS positions" in sql


def test_select_with_score_compile():
    base = select(products.c.id).where(search.match_any(products.c.description, "running"))
    stmt = select_with.score(base, products.c.id, label="search_score")
    sql = _sql(stmt)
    assert "pdb.score(products.id) AS search_score" in sql


def test_match_all_requires_terms():
    with pytest.raises(ValueError, match="at least one search term"):
        search.match_all(products.c.description)


def test_snippet_requires_both_tags():
    with pytest.raises(ValueError, match="provided together"):
        pdb.snippet(products.c.description, start_tag="<mark>")
