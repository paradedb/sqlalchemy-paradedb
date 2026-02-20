from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, Text, column, select, table
from sqlalchemy.dialects import postgresql

from paradedb.sqlalchemy import facets, search
from paradedb.sqlalchemy._functions import PDBFunctionWithNamedArgs
from paradedb.sqlalchemy._pdb_cast import PDBCast
from paradedb.sqlalchemy.errors import (
    FacetRequiresLimitError,
    FacetRequiresOrderByError,
    FacetRequiresParadeDBPredicateError,
    InvalidArgumentError,
    InvalidMoreLikeThisOptionsError,
)


products = table(
    "products",
    column("id", Integer),
    column("description", Text),
    column("category", String),
)


def _sql(stmt) -> str:
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def test_search_argument_validation_errors():
    with pytest.raises(InvalidArgumentError, match="distance must be between 0 and 2"):
        search.fuzzy(products.c.description, "oops", distance=3)

    with pytest.raises(InvalidArgumentError, match="slop must be >= 0"):
        search.phrase(products.c.description, "running shoes", slop=-1)

    with pytest.raises(InvalidArgumentError, match="max_expansions must be > 0"):
        search.phrase_prefix(products.c.description, ["running"], max_expansions=0)

    with pytest.raises(InvalidArgumentError, match="slop must be >= 0"):
        search.regex_phrase(products.c.description, ["run.*"], slop=-1)

    with pytest.raises(InvalidArgumentError, match="distance must be >= 0"):
        search.near(products.c.description, "running", "shoes", distance=-1)

    with pytest.raises(InvalidArgumentError, match="max_expansions must be >= 0"):
        search.prox_regex("sho.*", max_expansions=-1)

    with pytest.raises(InvalidArgumentError, match="cannot be used together"):
        search.near(
            products.c.description,
            "running",
            "shoes",
            distance=1,
            right_pattern="sho.*",
        )

    with pytest.raises(InvalidArgumentError, match="right is required"):
        search.near(products.c.description, "running", distance=1)


def test_more_like_this_uses_specific_error_type():
    with pytest.raises(InvalidMoreLikeThisOptionsError, match="exactly one"):
        search.more_like_this(products.c.id)

    with pytest.raises(InvalidMoreLikeThisOptionsError, match="stopwords entries"):
        search.more_like_this(products.c.id, document_id=1, stopwords=[""])


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
    base = select(products.c.id).where(search.match_all(products.c.description, "running")).order_by(products.c.id).limit(5)
    stmt, _ = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=products.c.id)
    sql = _sql(stmt)
    assert "pdb.all()" not in sql


def test_custom_nodes_have_cache_keys():
    cast_expr = PDBCast(products.c.description, "boost", (2,))
    fn_expr = PDBFunctionWithNamedArgs("agg", [products.c.id], [("approximate", False)])

    cast_key = cast_expr._generate_cache_key()
    fn_key = fn_expr._generate_cache_key()

    assert cast_key is not None
    assert fn_key is not None
