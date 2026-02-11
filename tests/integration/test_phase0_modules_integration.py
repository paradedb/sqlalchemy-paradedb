from __future__ import annotations

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Integer, MetaData, Table, Text, Column, select, text, func

from conftest import Product
from paradedb.sqlalchemy import expr as pdb_expr
from paradedb.sqlalchemy import inspect as pdb_inspect
from paradedb.sqlalchemy import search
from paradedb.sqlalchemy.errors import DuplicateTokenizerAliasError, InvalidArgumentError
from paradedb.sqlalchemy.indexing import BM25Field, tokenize
import paradedb.sqlalchemy.alembic  # noqa: F401  Ensures Alembic ops registration


pytestmark = pytest.mark.integration


def test_expr_helpers_execute(session):
    concat_stmt = select(pdb_expr.concat_ws(" ", Product.category, Product.description)).where(Product.id == 3)
    concat_value = session.execute(concat_stmt).scalar_one()
    assert concat_value.startswith("Electronics Wireless")

    json_stmt = select(pdb_expr.json_text(func.jsonb_build_object("kind", "shoe"), "kind"))
    json_value = session.execute(json_stmt).scalar_one()
    assert json_value == "shoe"


def test_inspect_detects_paradedb_predicates():
    stmt = select(Product.id).where(search.match_all(Product.description, "running", "shoes"))
    assert pdb_inspect.has_paradedb_predicate(stmt)
    assert "&&&" in pdb_inspect.collect_paradedb_operators(stmt)

    plain_stmt = select(Product.id).where(Product.rating >= 4)
    assert not pdb_inspect.has_paradedb_predicate(plain_stmt)
    assert pdb_inspect.collect_paradedb_operators(plain_stmt) == set()


def test_custom_errors_raised_for_validation(engine):
    metadata = MetaData()
    table_name = "phase0_error_products"
    index_name = "phase0_error_products_bm25_idx"

    with engine.begin() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
    )
    metadata.create_all(engine)

    from sqlalchemy import Index

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.unicode(alias="dup")),
        BM25Field(products.c.description, tokenizer=tokenize.literal(alias="dup")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(DuplicateTokenizerAliasError):
        idx.create(engine)

    with pytest.raises(InvalidArgumentError):
        search.more_like_this(products.c.id)

    with engine.begin() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def test_alembic_ops_create_reindex_drop(engine):
    table_name = "phase0_alembic_products"
    index_name = "phase0_alembic_products_bm25_idx"

    with engine.begin() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        conn.execute(text(f"CREATE TABLE {table_name} (id int primary key, description text not null)"))

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        op.create_bm25_index(index_name, table_name, ["id", "description"], key_field="id")

        exists = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pg_indexes
                WHERE tablename = :table_name
                  AND indexname = :index_name
                """
            ),
            {"table_name": table_name, "index_name": index_name},
        ).scalar_one()
        assert exists == 1

        op.reindex_bm25(index_name)
        op.drop_bm25_index(index_name, if_exists=True)

        exists_after_drop = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pg_indexes
                WHERE tablename = :table_name
                  AND indexname = :index_name
                """
            ),
            {"table_name": table_name, "index_name": index_name},
        ).scalar_one()
        assert exists_after_drop == 0

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
