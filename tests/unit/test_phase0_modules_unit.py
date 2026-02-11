from __future__ import annotations

from sqlalchemy import Integer, String, Text, and_, column, select, table
from sqlalchemy.dialects import postgresql

from paradedb.sqlalchemy import expr as pdb_expr
from paradedb.sqlalchemy import inspect as pdb_inspect
from paradedb.sqlalchemy import search
from paradedb.sqlalchemy.errors import (
    DuplicateTokenizerAliasError,
    InvalidArgumentError,
    ParadeDBError,
)


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
