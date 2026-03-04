from __future__ import annotations

import pytest
from sqlalchemy import Integer, String, Text, and_, column, select, table
from sqlalchemy.dialects import postgresql

from paradedb.sqlalchemy import expr as pdb_expr
from paradedb.sqlalchemy import inspect as pdb_inspect
from paradedb.sqlalchemy import search
from paradedb.sqlalchemy.errors import (
    DuplicateTokenizerAliasError,
    InvalidArgumentError,
    ParadeDBError,
    SnippetWithFuzzyPredicateError,
)
from paradedb.sqlalchemy import select_with


products = table(
    "products",
    column("id", Integer),
    column("description", Text),
    column("category", String),
)


def _sql(stmt) -> str:
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def test_error_hierarchy():
    assert issubclass(InvalidArgumentError, ParadeDBError)
    assert issubclass(InvalidArgumentError, ValueError)
    assert issubclass(DuplicateTokenizerAliasError, ValueError)
    assert issubclass(SnippetWithFuzzyPredicateError, ParadeDBError)


def test_expr_helpers_compile():
    concat_stmt = select(pdb_expr.concat_ws(" ", products.c.category, products.c.description))
    json_stmt = select(pdb_expr.json_text(products.c.description.cast(postgresql.JSONB), "kind"))

    concat_sql = _sql(concat_stmt)
    json_sql = _sql(json_stmt)

    assert "concat_ws(' ', products.category, products.description)" in concat_sql
    assert "CAST(products.description AS JSONB) ->> 'kind'" in json_sql


def test_inspect_detects_predicates_in_boolean_tree():
    stmt = select(products.c.id).where(
        and_(
            search.match_all(products.c.description, "running"),
            search.match_any(products.c.category, "Footwear"),
        )
    )

    ops = pdb_inspect.collect_paradedb_operators(stmt)
    assert ops == {"&&&", "|||"}
    assert pdb_inspect.has_paradedb_predicate(stmt)


def test_inspect_no_predicate_for_plain_sql():
    stmt = select(products.c.id).where(products.c.id > 1)
    assert pdb_inspect.collect_paradedb_operators(stmt) == set()
    assert not pdb_inspect.has_paradedb_predicate(stmt)


def test_inspect_detects_fuzzy_predicate():
    fuzzy_stmt = select(products.c.id).where(search.match_any(products.c.description, "wirless", distance=1))
    boosted_fuzzy_stmt = select(products.c.id).where(
        search.match_any(products.c.description, "wirless", distance=1, boost=2)
    )
    non_fuzzy_stmt = select(products.c.id).where(search.term(products.c.description, "wireless"))

    assert pdb_inspect.has_fuzzy_predicate(fuzzy_stmt)
    assert pdb_inspect.has_fuzzy_predicate(boosted_fuzzy_stmt)
    assert not pdb_inspect.has_fuzzy_predicate(non_fuzzy_stmt)


def test_select_with_snippet_guard_raises_on_fuzzy():
    base = select(products.c.id, products.c.description).where(
        search.match_any(products.c.description, "wirless", distance=1)
    )
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippet(base, products.c.description)


def test_select_with_snippet_positions_guard_raises_on_fuzzy():
    base = select(products.c.id, products.c.description).where(
        search.match_any(products.c.description, "wirless", distance=1)
    )
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippet_positions(base, products.c.description)
