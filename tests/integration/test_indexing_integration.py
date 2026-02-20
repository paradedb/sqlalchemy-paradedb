from __future__ import annotations

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from paradedb.sqlalchemy.expr import json_text
from paradedb.sqlalchemy.indexing import BM25Field, assert_indexed, describe, tokenize
from paradedb.sqlalchemy.errors import FieldNotIndexedError


pytestmark = pytest.mark.integration


def _drop_table_and_index(engine, table_name: str, index_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS {index_name}'))
        conn.execute(text(f'DROP TABLE IF EXISTS {table_name}'))


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
    except Exception:
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
