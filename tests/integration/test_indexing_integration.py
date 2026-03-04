from __future__ import annotations

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError

from paradedb.sqlalchemy.expr import json_text
from paradedb.sqlalchemy.indexing import BM25Field, assert_indexed, describe, tokenize
from paradedb.sqlalchemy.errors import FieldNotIndexedError


pytestmark = pytest.mark.integration


def _drop_table_and_index(engine, table_name: str, index_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def _tokenizer_cast_supported(engine) -> bool:
    table_name = "__idx_syntax_test"
    index_name = "__idx_syntax_test_idx"
    _drop_table_and_index(engine, table_name, index_name)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {table_name} (id int primary key, description text not null)"))
            conn.execute(
                text(
                    f"""
                    CREATE INDEX {index_name}
                    ON {table_name}
                    USING bm25 (id, (description::pdb.unicode_words('lowercase=true')))
                    WITH (key_field='id')
                    """
                )
            )
        return True
    except SQLAlchemyError:
        return False
    finally:
        _drop_table_and_index(engine, table_name, index_name)


def test_bm25_index_create(engine):
    table_name = "indexing_products"
    index_name = "indexing_products_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("category", String(120), nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        BM25Field(products.c.category),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT indexdef
                FROM pg_indexes
                WHERE tablename = :table_name
                  AND indexname = :index_name
                """
            ),
            {"table_name": table_name, "index_name": index_name},
        ).one()

    assert "USING bm25" in row.indexdef
    assert "description" in row.indexdef
    assert "category" in row.indexdef

    _drop_table_and_index(engine, table_name, index_name)


def test_bm25_index_with_tokenizers_when_supported(engine):
    if not _tokenizer_cast_supported(engine):
        pytest.skip("ParadeDB instance does not support tokenizer cast index syntax yet")

    table_name = "indexing_products_tokenized"
    index_name = "indexing_products_tokenized_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("category", String(120), nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.unicode(lowercase=True)),
        BM25Field(products.c.category, tokenizer=tokenize.literal_normalized(alias="category_exact")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT indexdef
                FROM pg_indexes
                WHERE tablename = :table_name
                  AND indexname = :index_name
                """
            ),
            {"table_name": table_name, "index_name": index_name},
        ).one()

    assert "pdb.unicode_words" in row.indexdef
    assert "lowercase=true" in row.indexdef
    assert "pdb.literal_normalized" in row.indexdef

    _drop_table_and_index(engine, table_name, index_name)


def test_bm25_index_json_keys_when_supported(engine):
    if not _tokenizer_cast_supported(engine):
        pytest.skip("ParadeDB instance does not support tokenizer cast index syntax yet")

    table_name = "indexing_products_json"
    index_name = "indexing_products_json_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("metadata", JSONB, nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(
            json_text(products.c.metadata, "color"),
            tokenizer=tokenize.literal(alias="metadata_color"),
        ),
        BM25Field(
            json_text(products.c.metadata, "location"),
            tokenizer=tokenize.literal(alias="metadata_location"),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT indexdef
                FROM pg_indexes
                WHERE tablename = :table_name
                  AND indexname = :index_name
                """
            ),
            {"table_name": table_name, "index_name": index_name},
        ).one()

    assert "->>" in row.indexdef
    assert "'color'" in row.indexdef
    assert "'location'" in row.indexdef
    assert row.indexdef.count("pdb.literal(") >= 2

    _drop_table_and_index(engine, table_name, index_name)


def test_create_all_with_attached_bm25_index(engine):
    table_name = "attached_index_items"
    index_name = "attached_index_items_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    items = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
    )
    Index(
        index_name,
        BM25Field(items.c.id),
        BM25Field(items.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    metadata.create_all(engine)

    with engine.begin() as conn:
        count = conn.execute(
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

    assert count == 1

    _drop_table_and_index(engine, table_name, index_name)


def test_duplicate_tokenizer_alias_is_rejected(engine):
    table_name = "invalid_alias_products"
    index_name = "invalid_alias_products_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.unicode(alias="desc_alias", lowercase=True)),
        BM25Field(products.c.description, tokenizer=tokenize.literal(alias="desc_alias")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="Duplicate tokenizer alias"):
        idx.create(engine)

    _drop_table_and_index(engine, table_name, index_name)


def test_missing_key_field_is_rejected(engine):
    table_name = "invalid_key_field_products"
    index_name = "invalid_key_field_products_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        postgresql_using="bm25",
    )

    with pytest.raises(ValueError, match="key_field"):
        idx.create(engine)

    _drop_table_and_index(engine, table_name, index_name)


def test_describe_returns_fields_and_aliases(engine):
    table_name = "describe_products"
    index_name = "describe_products_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("category", String(120), nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.unicode(lowercase=True)),
        BM25Field(products.c.category, tokenizer=tokenize.literal_normalized(alias="category_exact")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    metas = describe(engine, products)
    meta = next(m for m in metas if m.index_name == index_name)

    assert meta.key_field == "id"
    assert meta.fields == ("id", "description", "category")
    assert meta.aliases == {"category_exact": "category"}

    _drop_table_and_index(engine, table_name, index_name)


def test_describe_includes_tokenizers(engine):
    """describe() populates IndexMeta.tokenizers from the index definition."""
    if not _tokenizer_cast_supported(engine):
        pytest.skip("ParadeDB instance does not support tokenizer cast index syntax yet")

    table_name = "describe_tokenizers_products"
    index_name = "describe_tokenizers_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("category", String(120), nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.unicode(lowercase=True)),
        BM25Field(products.c.category, tokenizer=tokenize.literal()),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    metas = describe(engine, products)
    meta = next(m for m in metas if m.index_name == index_name)

    assert "unicode_words" in meta.tokenizers.get("description", ())
    assert "literal" in meta.tokenizers.get("category", ())
    assert "id" not in meta.tokenizers  # no tokenizer for plain key field

    _drop_table_and_index(engine, table_name, index_name)


def test_describe_and_assert_indexed_for_json_expression_tokenizer(engine):
    """JSON expression BM25 fields map back to the base column for introspection checks."""
    if not _tokenizer_cast_supported(engine):
        pytest.skip("ParadeDB instance does not support tokenizer cast index syntax yet")

    table_name = "describe_json_expr_products"
    index_name = "describe_json_expr_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("metadata", JSONB, nullable=False),
        Column("extra", Text, nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(
            json_text(products.c.metadata, "color"),
            tokenizer=tokenize.literal(alias="metadata_color"),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    metas = describe(engine, products)
    meta = next(m for m in metas if m.index_name == index_name)

    assert meta.fields == ("id", "metadata")
    assert "literal" in meta.tokenizers.get("metadata", ())

    assert_indexed(engine, products.c.metadata)
    assert_indexed(engine, products.c.metadata, tokenizer="literal")
    with pytest.raises(FieldNotIndexedError, match="'extra'"):
        assert_indexed(engine, products.c.extra)

    _drop_table_and_index(engine, table_name, index_name)


def test_assert_indexed_passes_and_raises(engine):
    """assert_indexed passes for an indexed column and raises for an unindexed one."""
    table_name = "assert_indexed_products"
    index_name = "assert_indexed_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    tbl = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("extra", Text, nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(tbl.c.id),
        BM25Field(tbl.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    # 'description' is indexed → no error
    assert_indexed(engine, tbl.c.description)

    # 'extra' is not indexed → FieldNotIndexedError
    with pytest.raises(FieldNotIndexedError, match="'extra'"):
        assert_indexed(engine, tbl.c.extra)

    _drop_table_and_index(engine, table_name, index_name)


def test_describe_and_assert_indexed_with_explicit_schema(engine):
    schema_name = "indexing_schema"
    table_name = "schema_products"
    index_name = "schema_products_bm25_idx"

    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("extra", Text, nullable=False),
        schema=schema_name,
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    idx.create(engine)

    # Table carries schema; describe/assert_indexed should resolve correctly.
    metas = describe(engine, products)
    assert any(m.index_name == index_name for m in metas)
    assert_indexed(engine, products.c.description)
    with pytest.raises(FieldNotIndexedError, match="'extra'"):
        assert_indexed(engine, products.c.extra)

    # Schema override should work for an unqualified table definition too.
    unqualified_meta = MetaData()
    unqualified = Table(
        table_name,
        unqualified_meta,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
    )
    metas_override = describe(engine, unqualified, schema=schema_name)
    assert any(m.index_name == index_name for m in metas_override)
    assert_indexed(engine, unqualified.c.description, schema=schema_name)

    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


def test_bm25_partial_index_generates_where_clause(engine):
    """A BM25 index with postgresql_where= includes a WHERE clause in the DDL."""
    table_name = "partial_bm25_products"
    index_name = "partial_bm25_products_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("rating", Integer, nullable=False),
    )
    metadata.create_all(engine)

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
        postgresql_where=products.c.rating > 3,
    )
    idx.create(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE tablename = :t AND indexname = :i"),
            {"t": table_name, "i": index_name},
        ).one()

    assert "WHERE" in row.indexdef, f"Expected WHERE in indexdef: {row.indexdef}"
    assert "rating" in row.indexdef

    _drop_table_and_index(engine, table_name, index_name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def test_bm25_partial_index_filters_search_results(engine):
    """Rows excluded by the partial index condition are not found via BM25 search."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from paradedb.sqlalchemy.search import match_all

    table_name = "partial_search_products"
    index_name = "partial_search_products_bm25_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("rating", Integer, nullable=False),
    )
    metadata.create_all(engine)

    # Only index rows where rating > 3
    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
        postgresql_where=products.c.rating > 3,
    )
    idx.create(engine)

    with engine.begin() as conn:
        conn.execute(
            products.insert(),
            [
                {"id": 1, "description": "premium running shoes", "rating": 5},
                {"id": 2, "description": "budget running shoes", "rating": 2},
            ],
        )

    with Session(engine) as session:
        stmt = select(products.c.id).where(match_all(products.c.description, "running"))
        ids = [row.id for row in session.execute(stmt)]

    # id=1 (rating 5) is indexed; id=2 (rating 2) is excluded by the partial condition
    assert 1 in ids, f"Expected id=1 in results: {ids}"
    assert 2 not in ids, f"Expected id=2 excluded from partial index results: {ids}"

    _drop_table_and_index(engine, table_name, index_name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def test_bm25_index_create_concurrently(engine):
    """BM25 index can be created with postgresql_concurrently=True."""
    table_name = "concurrent_bm25_products"
    index_name = "concurrent_bm25_products_idx"
    _drop_table_and_index(engine, table_name, index_name)

    metadata = MetaData()
    products = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
    )
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(products.insert(), [{"id": 1, "description": "test item"}])

    idx = Index(
        index_name,
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
        postgresql_concurrently=True,
    )
    # CONCURRENTLY cannot run inside a transaction block; use autocommit mode.
    idx.create(engine.execution_options(isolation_level="AUTOCOMMIT"))

    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM pg_indexes WHERE tablename = :t AND indexname = :i"),
            {"t": table_name, "i": index_name},
        ).scalar_one()

    assert count == 1, "Expected concurrent BM25 index to be created"

    _drop_table_and_index(engine, table_name, index_name)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
