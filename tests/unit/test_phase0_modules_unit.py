from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, and_, column, select, table
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.schema import CreateTable

from paradedb import tokenizer
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
from paradedb.sqlalchemy.vector import Vector


products = table(
    "products",
    column("id", Integer),
    column("description", Text),
    column("category", String),
)


def _sql(stmt) -> str:
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    return "\n".join(line.rstrip() for line in sql.split("\n")).strip()


def test_error_hierarchy():
    assert issubclass(InvalidArgumentError, ParadeDBError)
    assert issubclass(InvalidArgumentError, ValueError)
    assert issubclass(DuplicateTokenizerAliasError, ValueError)
    assert issubclass(SnippetWithFuzzyPredicateError, ParadeDBError)


def test_expr_helpers_compile():
    concat_stmt = select(pdb_expr.concat_ws(" ", products.c.category, products.c.description))
    json_stmt = select(pdb_expr.json_text(products.c.description.cast(postgresql.JSONB), "kind"))

    assert (
        _sql(concat_stmt)
        == """\
SELECT concat_ws(' ', products.category, products.description) AS concat_ws_1
FROM products"""
    )
    assert (
        _sql(json_stmt)
        == """\
SELECT CAST(products.description AS JSONB) ->> 'kind' AS anon_1
FROM products"""
    )


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
    tokenized_fuzzy_stmt = select(products.c.id).where(
        search.match_any(
            products.c.description,
            "wirless",
            distance=1,
            tokenizer=tokenizer.regex_pattern(pattern=r"[^\s]+"),
        )
    )
    non_fuzzy_stmt = select(products.c.id).where(search.term(products.c.description, "wireless"))

    assert pdb_inspect.has_fuzzy_predicate(fuzzy_stmt)
    assert pdb_inspect.has_fuzzy_predicate(boosted_fuzzy_stmt)
    assert pdb_inspect.has_fuzzy_predicate(tokenized_fuzzy_stmt)
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


def test_vector_column_ddl_compile():
    items = Table(
        "items",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("embedding", Vector(3)),
    )
    ddl = _sql(CreateTable(items))
    assert "embedding vector(3)" in ddl


def test_vector_column_ddl_without_dim():
    assert Vector().get_col_spec() == "vector"


def test_vector_requires_positive_dim():
    with pytest.raises(ValueError, match="dim"):
        Vector(0)


def test_vector_bind_processor_serializes_sequences():
    process = Vector(3).bind_processor(postgresql.dialect())
    assert process([1, 0, 0.5]) == "[1.0,0.0,0.5]"
    assert process("[1,2,3]") == "[1,2,3]"
    assert process(None) is None


def test_vector_bind_processor_rejects_non_sequences():
    process = Vector(3).bind_processor(postgresql.dialect())
    with pytest.raises(InvalidArgumentError, match="sequence of numbers"):
        process(42)


def test_vector_result_processor_parses_text():
    process = Vector(3).result_processor(postgresql.dialect(), None)
    assert process("[1,2.5,3]") == [1.0, 2.5, 3.0]
    assert process("[]") == []
    assert process(None) is None
    assert process([1, 2]) == [1.0, 2.0]


def test_vector_registered_for_reflection():
    assert ischema_names["vector"] is Vector
