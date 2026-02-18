from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, Text, and_, not_, or_, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import column, table
from sqlalchemy.orm import aliased

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


def test_parse_phrase_prefix_regex_phrase_compile():
    parse_stmt = select(products.c.id).where(search.parse(products.c.id, "description:sleek", lenient=True))
    phrase_prefix_stmt = select(products.c.id).where(search.phrase_prefix(products.c.description, ["running", "sh"]))
    regex_phrase_stmt = select(products.c.id).where(
        search.regex_phrase(products.c.description, ["run.*", "shoe.*"], slop=1, max_expansions=100)
    )

    parse_sql = _sql(parse_stmt)
    phrase_prefix_sql = _sql(phrase_prefix_stmt)
    regex_phrase_sql = _sql(regex_phrase_stmt)

    assert "id @@@ pdb.parse('description:sleek', true, false)" in parse_sql
    assert "description @@@ pdb.phrase_prefix(ARRAY['running', 'sh'], 50)" in phrase_prefix_sql
    assert "description @@@ pdb.regex_phrase(ARRAY['run.*', 'shoe.*'], 1, 100)" in regex_phrase_sql


def test_near_and_proximity_compile():
    near_stmt = select(products.c.id).where(search.near(products.c.description, "sleek", "shoes", distance=1))
    prox_stmt = select(products.c.id).where(
        search.proximity(
            products.c.description,
            search.prox_array(search.prox_regex("sl.*"), "running").near("shoes", distance=1),
        )
    )

    near_sql = _sql(near_stmt)
    prox_sql = _sql(prox_stmt)

    assert "description @@@" in near_sql
    assert "'sleek'::pdb.proximityclause ## 1" in near_sql
    assert "## 'shoes'::pdb.proximityclause" in near_sql
    assert "pdb.prox_regex('sl.*', 100)" in prox_sql
    assert "pdb.prox_array" in prox_sql
    assert "## 1" in prox_sql


def test_more_like_this_compile():
    by_id_stmt = select(products.c.id).where(search.more_like_this(products.c.id, document_id=3, fields=["description"]))
    by_doc_stmt = select(products.c.id).where(
        search.more_like_this(products.c.id, document={"description": "wireless earbuds"})
    )
    with_opts_stmt = select(products.c.id).where(
        search.more_like_this(
            products.c.id,
            document_id=3,
            fields=["description"],
            min_term_frequency=2,
            max_query_terms=10,
            stopwords=["the", "a"],
        )
    )

    by_id_sql = _sql(by_id_stmt)
    by_doc_sql = _sql(by_doc_stmt)
    with_opts_sql = _sql(with_opts_stmt)

    assert "id @@@ pdb.more_like_this(3, ARRAY['description'])" in by_id_sql
    assert "id @@@ pdb.more_like_this('{\"description\":\"wireless earbuds\"}')" in by_doc_sql
    assert "id @@@ pdb.more_like_this(3, ARRAY['description'], 2, 10" in with_opts_sql
    assert "ARRAY['the', 'a']" in with_opts_sql


def test_more_like_this_requires_exactly_one_source():
    with pytest.raises(ValueError, match="exactly one"):
        search.more_like_this(products.c.id)
    with pytest.raises(ValueError, match="exactly one"):
        search.more_like_this(products.c.id, document_id=1, document={"description": "x"})


def test_alias_subquery_cte_compile():
    ProductAlias = aliased(products, name="p_alias")
    aliased_stmt = (
        select(ProductAlias.c.id)
        .where(search.match_any(ProductAlias.c.description, "running"))
    )

    sq = select(products.c.id.label("pid"), products.c.description.label("description")).subquery("sq")
    sq_stmt = select(sq.c.pid).where(search.match_all(sq.c.description, "running", "shoes"))

    cte = select(products.c.id.label("pid"), products.c.description.label("description")).cte("base")
    cte_stmt = select(cte.c.pid).where(search.match_any(cte.c.description, "wireless"))

    bool_stmt = select(products.c.id).where(
        or_(
            and_(
                search.match_all(products.c.description, "running"),
                not_(search.match_any(products.c.description, "trail")),
            ),
            search.match_any(products.c.category, "Electronics"),
        )
    )

    aliased_sql = _sql(aliased_stmt)
    sq_sql = _sql(sq_stmt)
    cte_sql = _sql(cte_stmt)
    bool_sql = _sql(bool_stmt)

    assert "p_alias.description ||| 'running'" in aliased_sql
    assert "sq.description &&& ARRAY['running', 'shoes']" in sq_sql
    assert "base.description ||| 'wireless'" in cte_sql
    assert "NOT (products.description ||| 'trail')" in bool_sql
