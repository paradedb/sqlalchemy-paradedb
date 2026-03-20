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
    column("rating", Integer),
)


def _sql(stmt) -> str:
    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    return "\n".join(line.rstrip() for line in sql.split("\n")).strip()


def test_match_all_multiple_terms_compile():
    stmt = select(products.c.id).where(search.match_all(products.c.description, "running", "shoes"))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description &&& ARRAY['running', 'shoes']"""
    )


def test_phrase_with_slop_and_boost_compile():
    stmt = select(products.c.id).where(search.phrase(products.c.description, "running shoes", slop=2, boost=3))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ### 'running shoes'::pdb.slop(2)::pdb.boost(3)"""
    )


def test_term_fuzzy_compile_with_options():
    stmt = select(products.c.id).where(
        search.term(products.c.description, "shose", distance=1, prefix=False, transpose_cost_one=True)
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description === 'shose'::pdb.fuzzy(1, f, t)"""
    )


def test_match_any_fuzzy_compile():
    stmt = select(products.c.id).where(
        search.match_any(products.c.description, "running", "shose", distance=1, prefix=True)
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ||| ARRAY['running', 'shose']::pdb.fuzzy(1, t)"""
    )


def test_term_fuzzy_compile_with_transpose_implicit_prefix_slot():
    stmt = select(products.c.id).where(
        search.term(products.c.description, "shose", distance=1, transpose_cost_one=True)
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description === 'shose'::pdb.fuzzy(1, f, t)"""
    )


def test_regex_and_all_compile():
    regex_stmt = select(products.c.id).where(search.regex(products.c.description, "run.*"))
    all_stmt = select(products.c.id).where(search.all(products.c.id))
    regex_sql = _sql(regex_stmt)
    all_sql = _sql(all_stmt)

    assert (
        regex_sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ pdb.regex('run.*')"""
    )
    assert (
        all_sql
        == """SELECT products.id
FROM products
WHERE products.id @@@ pdb.all()"""
    )


def test_match_any_with_tokenizer_compile():
    stmt = select(products.c.id).where(
        search.match_any(products.c.description, "running shoes", tokenizer="whitespace")
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ||| 'running shoes'::pdb.whitespace"""
    )


def test_phrase_with_tokenizer_compile():
    stmt = select(products.c.id).where(search.phrase(products.c.description, "running shoes", tokenizer="whitespace"))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ### 'running shoes'::pdb.whitespace"""
    )


def test_phrase_pretokenized_with_slop_compile():
    stmt = select(products.c.id).where(search.phrase(products.c.description, ["shoes", "running"], slop=2))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ### CAST(ARRAY['shoes', 'running'] AS TEXT[])::pdb.slop(2)"""
    )


def test_phrase_with_slop_and_const_compile():
    stmt = select(products.c.id).where(search.phrase(products.c.description, "running shoes", slop=2, const=1.0))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ### 'running shoes'::pdb.slop(2)::pdb.query::pdb.const(1.0)"""
    )


def test_regex_boost_compile():
    stmt = select(products.c.id).where(search.regex(products.c.description, "key.*", boost=2.0))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ pdb.regex('key.*')::pdb.boost(2.0)"""
    )


def test_match_any_fuzzy_with_const_compile():
    stmt = select(products.c.id).where(search.match_any(products.c.description, "shose", distance=2, const=1.0))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ||| 'shose'::pdb.fuzzy(2)::pdb.query::pdb.const(1.0)"""
    )


def test_match_any_const_compile():
    stmt = select(products.c.id).where(search.match_any(products.c.description, "shoes", const=1.0))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description ||| 'shoes'::pdb.const(1.0)"""
    )


def test_exists_compile():
    stmt = select(products.c.id).where(search.exists(products.c.rating))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.rating @@@ pdb.exists()"""
    )


def test_pdb_helpers_compile():
    stmt = select(
        pdb.score(products.c.id).label("score"),
        pdb.snippet(products.c.description, start_tag="<mark>", end_tag="</mark>", max_num_chars=100).label("snippet"),
        pdb.snippets(
            products.c.description,
            start_tag="[",
            end_tag="]",
            max_num_chars=15,
            limit=1,
            offset=0,
            sort_by="position",
        ).label("snippets"),
        pdb.snippet_positions(products.c.description).label("positions"),
    )
    sql = _sql(stmt)

    assert (
        sql
        == """SELECT pdb.score(products.id) AS score, pdb.snippet(products.description, '<mark>', '</mark>', 100) AS snippet, pdb.snippets(products.description, start_tag => '[', end_tag => ']', max_num_chars => 15, "limit" => 1, "offset" => 0, sort_by => 'position') AS snippets, pdb.snippet_positions(products.description) AS positions
FROM products"""
    )


def test_select_with_score_compile():
    base = select(products.c.id).where(search.match_any(products.c.description, "running"))
    stmt = select_with.score(base, products.c.id, label="search_score")
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id, pdb.score(products.id) AS search_score
FROM products
WHERE products.description ||| 'running'"""
    )


def test_select_with_snippet_positions_compile():
    base = select(products.c.id, products.c.description).where(search.match_any(products.c.description, "running"))
    stmt = select_with.snippet_positions(base, products.c.description, label="positions")
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id, products.description, pdb.snippet_positions(products.description) AS positions
FROM products
WHERE products.description ||| 'running'"""
    )


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

    assert (
        parse_sql
        == """SELECT products.id
FROM products
WHERE products.id @@@ pdb.parse('description:sleek', true, false)"""
    )
    assert (
        phrase_prefix_sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ pdb.phrase_prefix(ARRAY['running', 'sh'], 50)"""
    )
    assert (
        regex_phrase_sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ pdb.regex_phrase(ARRAY['run.*', 'shoe.*'], 1, 100)"""
    )


def test_simple_proximity_query():
    prox_stmt = select(products.c.id).where(
        search.proximity_query(products.c.description, search.proximity("running").within(2, "shoe"))
    )

    assert (
        _sql(prox_stmt)
        == """SELECT products.id
FROM products
WHERE products.description @@@ (('running' ## 2) ## 'shoe')"""
    )


def test_proximity_terms_are_escaped_properly():
    prox_stmt = select(products.c.id).where(
        search.proximity_query(products.c.description, search.proximity("running'").within(2, "sh'oe"))
    )

    assert (
        _sql(prox_stmt)
        == """SELECT products.id
FROM products
WHERE products.description @@@ (('running''' ## 2) ## 'sh''oe')"""
    )


def test_proximity_supports_right_associativity():
    stmt = select(products.c.id).where(
        search.proximity_query(
            products.c.description,
            search.proximity("running").within(1, search.proximity("shoe").within(2, "store")),
        )
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ (('running' ## 1) ## (('shoe' ## 2) ## 'store'))"""
    )


def test_proximity_query_with_regex_compile():
    stmt = select(products.c.id).where(
        search.proximity_query(products.c.description, search.prox_regex("sho.*", 80).within(2, "store"))
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ ((pdb.prox_regex('sho.*', 80) ## 2) ## 'store')"""
    )


def test_proximity_query_with_array_compile():
    stmt = select(products.c.id).where(
        search.proximity_query(products.c.description, search.prox_array("sleek", "running").within(1, "shoe"))
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ ((pdb.prox_array('sleek', 'running') ## 1) ## 'shoe')"""
    )


def test_proximity_query_with_ordered_near_compile():
    stmt = select(products.c.id).where(
        search.proximity_query(
            products.c.description,
            search.proximity("running").within(3, "shoes", ordered=True),
        )
    )
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ (('running' ##> 3) ##> 'shoes')"""
    )


def test_complex_proximity_query():
    prox_stmt = select(products.c.id).where(
        search.proximity_query(
            products.c.description,
            search.prox_array(search.prox_regex("sl.*"), "running")
            .within(1, "shoes")
            .within(2, "store", ordered=True)
            .within(3, search.proximity(search.prox_regex("right")).within(3, "associative")),
        )
    )

    prox_sql = _sql(prox_stmt)

    assert (
        prox_sql
        == """SELECT products.id
FROM products
WHERE products.description @@@ ((((((pdb.prox_array(pdb.prox_regex('sl.*', 100), 'running') ## 1) ## 'shoes') ##> 2) ##> 'store') ## 3) ## ((pdb.prox_regex('right', 100) ## 3) ## 'associative'))"""
    )


def test_more_like_this_compile():
    by_id_stmt = select(products.c.id).where(
        search.more_like_this(products.c.id, document_id=3, fields=["description"])
    )
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

    assert (
        by_id_sql
        == """SELECT products.id
FROM products
WHERE products.id @@@ pdb.more_like_this(3, ARRAY['description'])"""
    )
    assert (
        by_doc_sql
        == """SELECT products.id
FROM products
WHERE products.id @@@ pdb.more_like_this('{"description":"wireless earbuds"}')"""
    )
    assert (
        with_opts_sql
        == """SELECT products.id
FROM products
WHERE products.id @@@ pdb.more_like_this(3, ARRAY['description'], min_term_frequency => 2, max_query_terms => 10, stopwords => ARRAY['the', 'a'])"""
    )


def test_more_like_this_requires_exactly_one_source():
    with pytest.raises(ValueError, match="exactly one"):
        search.more_like_this(products.c.id)
    with pytest.raises(ValueError, match="exactly one"):
        search.more_like_this(products.c.id, document_id=1, document={"description": "x"})


def test_alias_subquery_cte_compile():
    ProductAlias = aliased(products, name="p_alias")
    aliased_stmt = select(ProductAlias.c.id).where(search.match_any(ProductAlias.c.description, "running"))

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

    assert (
        aliased_sql
        == """SELECT p_alias.id
FROM products AS p_alias
WHERE p_alias.description ||| 'running'"""
    )
    assert (
        sq_sql
        == """SELECT sq.pid
FROM (SELECT products.id AS pid, products.description AS description
FROM products) AS sq
WHERE sq.description &&& ARRAY['running', 'shoes']"""
    )
    assert (
        cte_sql
        == """WITH base AS
(SELECT products.id AS pid, products.description AS description
FROM products)
 SELECT base.pid
FROM base
WHERE base.description ||| 'wireless'"""
    )
    assert (
        bool_sql
        == """SELECT products.id
FROM products
WHERE products.description &&& 'running' AND NOT (products.description ||| 'trail') OR products.category ||| 'Electronics'"""
    )


# ---------------------------------------------------------------------------
# New feature: search.range_term
# ---------------------------------------------------------------------------


def test_range_term_default_relation_compile():
    stmt = select(products.c.id).where(search.range_term(products.c.id, "[3,9]"))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.id @@@ pdb.range_term('[3,9]', 'Intersects')"""
    )


def test_range_term_explicit_relation_compile():
    stmt = select(products.c.id).where(search.range_term(products.c.id, "(3,9]", relation="Contains"))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT products.id
FROM products
WHERE products.id @@@ pdb.range_term('(3,9]', 'Contains')"""
    )


def test_range_term_invalid_relation_raises():
    with pytest.raises(ValueError, match="relation must be one of"):
        search.range_term(products.c.id, "[3,9]", relation="BadRelation")


def test_range_term_scalar_compile():
    range_items = table("range_items", column("id", Integer), column("weight_range", postgresql.INT4RANGE))
    stmt = select(range_items.c.id).where(search.range_term(range_items.c.weight_range, 1))
    sql = _sql(stmt)
    assert (
        sql
        == """SELECT range_items.id
FROM range_items
WHERE range_items.weight_range @@@ pdb.range_term(1)"""
    )


# ---------------------------------------------------------------------------
# New feature: indexing.validate_pushdown
# ---------------------------------------------------------------------------


def test_validate_pushdown_no_paradedb_predicate():
    from paradedb.sqlalchemy.indexing import validate_pushdown

    stmt = select(products.c.id).where(products.c.id > 3)
    warnings = validate_pushdown(stmt)
    assert any("No ParadeDB predicate" in w for w in warnings)


def test_validate_pushdown_no_where_clause():
    from paradedb.sqlalchemy.indexing import validate_pushdown

    stmt = select(products.c.id)
    warnings = validate_pushdown(stmt)
    assert any("No WHERE clause" in w for w in warnings)


def test_validate_pushdown_order_by_without_limit():
    from paradedb.sqlalchemy.indexing import validate_pushdown

    stmt = select(products.c.id).where(search.match_any(products.c.description, "running")).order_by(products.c.id)
    warnings = validate_pushdown(stmt)
    assert any("LIMIT" in w for w in warnings)


def test_validate_pushdown_clean_query_returns_empty():
    from paradedb.sqlalchemy.indexing import validate_pushdown

    stmt = (
        select(products.c.id)
        .where(search.match_any(products.c.description, "running"))
        .order_by(products.c.id)
        .limit(10)
    )
    warnings = validate_pushdown(stmt)
    assert warnings == []
