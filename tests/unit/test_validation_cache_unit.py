from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, Text, column, select, table
from sqlalchemy.dialects import postgresql

from paradedb.sqlalchemy import facets, pdb, search
from paradedb.sqlalchemy._functions import PDBFunctionWithNamedArgs
from paradedb.sqlalchemy._pdb_cast import PDBCast
from paradedb.sqlalchemy.errors import (
    FacetRequiresLimitError,
    FacetRequiresOrderByError,
    FacetRequiresParadeDBPredicateError,
    InvalidArgumentError,
    InvalidMoreLikeThisOptionsError,
)
from paradedb.sqlalchemy.indexing import validate_pushdown


products = table(
    "products",
    column("id", Integer),
    column("description", Text),
    column("category", String),
)


def _sql(stmt) -> str:
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    return "\n".join(line.rstrip() for line in sql.split("\n")).strip()


def test_search_argument_validation_errors():
    with pytest.raises(InvalidArgumentError, match="distance must be between 0 and 2"):
        search.term(products.c.description, "oops", distance=3)

    with pytest.raises(InvalidArgumentError, match="slop must be >= 0"):
        search.phrase(products.c.description, "running shoes", slop=-1)

    with pytest.raises(InvalidArgumentError, match="max_expansions must be > 0"):
        search.phrase_prefix(products.c.description, ["running"], max_expansions=0)

    with pytest.raises(InvalidArgumentError, match="slop must be >= 0"):
        search.regex_phrase(products.c.description, ["run.*"], slop=-1)

    with pytest.raises(InvalidArgumentError, match="distance must be >= 0"):
        search.prox_str("running").within(-1, "shoes")

    with pytest.raises(InvalidArgumentError, match="max_expansions must be >= 0"):
        search.prox_regex("sho.*", max_expansions=-1)

    with pytest.raises(InvalidArgumentError, match="tokenizer must be a bare identifier"):
        search.match_any(products.c.description, "running shoes", tokenizer="whitespace;drop")

    with pytest.raises(InvalidArgumentError, match="value must contain at least one token"):
        search.phrase(products.c.description, [])

    with pytest.raises(InvalidArgumentError, match="range_type is only supported"):
        search.range_term(products.c.id, 1, range_type="int4range")

    with pytest.raises(InvalidArgumentError, match="relation is only supported"):
        search.range_term(products.c.id, 1, relation="Contains")

    with pytest.raises(InvalidArgumentError, match="mutually exclusive"):
        search.match_any(products.c.description, "shoes", boost=2.0, const=1.0)


def test_more_like_this_uses_specific_error_type():
    with pytest.raises(InvalidMoreLikeThisOptionsError, match="exactly one"):
        search.more_like_this(products.c.id)

    with pytest.raises(InvalidMoreLikeThisOptionsError, match="document must not be empty"):
        search.more_like_this(products.c.id, document={})


def test_snippet_builder_validation_errors():
    with pytest.raises(InvalidArgumentError, match="max_num_chars must be > 0"):
        pdb.snippet(products.c.description, max_num_chars=0)

    with pytest.raises(InvalidArgumentError, match="limit must be > 0"):
        pdb.snippets(products.c.description, limit=0)

    with pytest.raises(InvalidArgumentError, match="offset must be >= 0"):
        pdb.snippets(products.c.description, offset=-1)

    with pytest.raises(InvalidArgumentError, match="sort_by must be a non-empty string"):
        pdb.snippets(products.c.description, sort_by="  ")

    with pytest.raises(InvalidArgumentError, match="spec must be a non-empty dict"):
        pdb.agg({})


def test_with_rows_guard_error_types():
    missing_order = select(products.c.id).limit(5)
    with pytest.raises(FacetRequiresOrderByError):
        facets.with_rows(missing_order, agg=facets.value_count(field="id"), key_field=products.c.id)

    missing_limit = select(products.c.id).order_by(products.c.id)
    with pytest.raises(FacetRequiresLimitError):
        facets.with_rows(missing_limit, agg=facets.value_count(field="id"), key_field=products.c.id)

    plain = select(products.c.id).order_by(products.c.id).limit(5)
    with pytest.raises(FacetRequiresParadeDBPredicateError):
        facets.with_rows(
            plain,
            agg=facets.value_count(field="id"),
            key_field=products.c.id,
            ensure_predicate=False,
        )


def test_with_rows_does_not_inject_sentinel_when_predicate_exists():
    base = (
        select(products.c.id)
        .where(search.match_all(products.c.description, "running"))
        .order_by(products.c.id)
        .limit(5)
    )
    stmt = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=products.c.id)
    assert (
        _sql(stmt)
        == """\
SELECT products.id, pdb.agg('{"value_count":{"field":"id"}}') OVER () AS facets
FROM products
WHERE products.description &&& 'running' ORDER BY products.id
 LIMIT 5"""
    )


def test_with_rows_limit_guard_ignores_limit_identifier_names():
    odd_table = table(
        "odd_products",
        column("id", Integer),
        column("limit", Integer),
    )
    stmt = select(odd_table.c.limit).order_by(odd_table.c.id)

    with pytest.raises(FacetRequiresLimitError):
        facets.with_rows(stmt, agg=facets.value_count(field="id"), key_field=odd_table.c.id)


def test_validate_pushdown_ignores_limit_identifier_names():
    odd_table = table(
        "odd_products",
        column("id", Integer),
        column("limit", Integer),
    )
    stmt = select(odd_table.c.limit).where(search.match_all(odd_table.c.id, "1")).order_by(odd_table.c.id)

    warnings = validate_pushdown(stmt)

    assert "ORDER BY is present without LIMIT; Top K pushdown to ParadeDB requires both" in warnings


def test_custom_nodes_have_cache_keys():
    cast_expr = PDBCast(products.c.description, "boost", (2,))
    fn_expr = PDBFunctionWithNamedArgs("agg", [products.c.id], [("approximate", False)])

    cast_key = cast_expr._generate_cache_key()
    fn_key = fn_expr._generate_cache_key()

    assert cast_key is not None
    assert fn_key is not None
