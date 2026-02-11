from __future__ import annotations

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateIndex

from paradedb.sqlalchemy.indexing import BM25Field, tokenize, validate_bm25_index


metadata = MetaData()
products = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("description", Text),
    Column("category", String),
)


def test_bm25_index_compile_with_tokenizers():
    idx = Index(
        "products_bm25_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.unicode(lowercase=True, stemmer="english")),
        BM25Field(products.c.category, tokenizer=tokenize.literal_normalized(alias="category_exact")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "USING bm25" in sql
    assert "(description::pdb.unicode_words('lowercase=true,stemmer=english'))" in sql
    assert "category::pdb.literal_normalized('alias=category_exact')" in sql
    assert "key_field = id" in sql


def test_bm25_field_non_postgres_compile_raises():
    with pytest.raises(CompileError, match="BM25Field is only supported"):
        str(BM25Field(products.c.id).compile(dialect=sqlite.dialect()))


def test_duplicate_alias_validation_raises():
    idx = Index(
        "products_bm25_alias_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.unicode(alias="description_alias")),
        BM25Field(products.c.category, tokenizer=tokenize.literal(alias="description_alias")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="Duplicate tokenizer alias"):
        validate_bm25_index(idx)


def test_key_field_validation_raises_when_missing():
    idx = Index(
        "products_bm25_missing_key_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        postgresql_using="bm25",
    )

    with pytest.raises(ValueError, match="key_field"):
        validate_bm25_index(idx)


def test_key_field_must_exist_in_fields():
    idx = Index(
        "products_bm25_bad_key_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "missing"},
    )

    with pytest.raises(ValueError, match="must match one of the indexed"):
        validate_bm25_index(idx)
