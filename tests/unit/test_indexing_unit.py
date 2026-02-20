from __future__ import annotations

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateIndex

from paradedb.sqlalchemy.indexing import (
    BM25Field,
    IndexMeta,
    _extract_alias,
    _extract_bm25_field_list,
    _extract_field_name,
    _extract_key_field,
    _extract_tokenizer_name,
    assert_indexed,
    tokenize,
    validate_bm25_index,
)
from paradedb.sqlalchemy.errors import FieldNotIndexedError, InvalidArgumentError
from paradedb.sqlalchemy.expr import json_text


metadata = MetaData()
products = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("description", Text),
    Column("category", String),
    Column("metadata", JSONB),
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
    assert "((description)::pdb.unicode_words('lowercase=true,stemmer=english'))" in sql
    assert "((category)::pdb.literal_normalized('alias=category_exact'))" in sql
    assert "key_field = id" in sql


def test_bm25_index_compile_with_structured_tokenizer_config():
    idx = Index(
        "products_bm25_structured_idx",
        BM25Field(products.c.id),
        BM25Field(
            products.c.description,
            tokenizer=tokenize.from_config(
                {
                    "tokenizer": "simple",
                    "filters": ["lowercase", "stemmer"],
                    "stemmer": "english",
                    "alias": "description_simple",
                }
            ),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "((description)::pdb.simple('alias=description_simple,lowercase=true,stemmer=english'))" in sql


def test_bm25_index_compile_with_tokenizer_positional_and_named_args():
    idx = Index(
        "products_bm25_ngram_idx",
        BM25Field(products.c.id),
        BM25Field(
            products.c.description,
            tokenizer=tokenize.from_config(
                {
                    "tokenizer": "ngram",
                    "args": [3, 8],
                    "named_args": {"prefix_only": True, "positions": True},
                    "alias": "description_ngram",
                }
            ),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "((description)::pdb.ngram(3,8,'alias=description_ngram,prefix_only=true,positions=true'))" in sql


def test_bm25_index_compile_with_pre_rendered_tokenizer_name_and_named_args():
    idx = Index(
        "products_bm25_ngram_inline_idx",
        BM25Field(products.c.id),
        BM25Field(
            products.c.description,
            tokenizer=tokenize.from_config(
                {
                    "tokenizer": "ngram(3,8)",
                    "named_args": {"prefix_only": True},
                }
            ),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "((description)::pdb.ngram(3,8)('prefix_only=true'))" in sql


def test_bm25_index_compile_lindera_wrapper():
    idx = Index(
        "products_bm25_lindera_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.lindera("japanese", alias="description_jp")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "((description)::pdb.lindera('japanese','alias=description_jp'))" in sql


def test_bm25_index_compile_regex_pattern_wrapper():
    idx = Index(
        "products_bm25_regex_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenize.regex_pattern(r"(?i)\\bh\\w*", alias="description_regex")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "((description)::pdb.regex_pattern('(?i)\\\\bh\\\\w*','alias=description_regex'))" in sql


def test_bm25_index_compile_json_key_with_tokenizer():
    idx = Index(
        "products_bm25_json_idx",
        BM25Field(products.c.id),
        BM25Field(
            json_text(products.c.metadata, "color"),
            tokenizer=tokenize.literal(alias="metadata_color"),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "((metadata ->> 'color')::pdb.literal('alias=metadata_color'))" in sql


def test_bm25_index_compile_multiple_json_keys():
    idx = Index(
        "products_bm25_json_multi_idx",
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
    sql = str(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "alias=metadata_color" in sql
    assert "alias=metadata_location" in sql


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


def test_key_field_must_be_first_field():
    idx = Index(
        "products_bm25_key_not_first_idx",
        BM25Field(products.c.description),
        BM25Field(products.c.id),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="must be the first indexed BM25Field"):
        validate_bm25_index(idx)


def test_key_field_must_be_untokenized():
    idx = Index(
        "products_bm25_key_tokenized_idx",
        BM25Field(products.c.id, tokenizer=tokenize.literal(alias="id_alias")),
        BM25Field(products.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="must be untokenized"):
        validate_bm25_index(idx)


def test_tokenizer_from_config_unknown_key_raises():
    with pytest.raises(InvalidArgumentError, match="Unknown tokenizer config keys"):
        tokenize.from_config({"tokenizer": "simple", "unknown": True})


def test_tokenizer_from_config_options_key_raises_as_unknown():
    with pytest.raises(InvalidArgumentError, match="Unknown tokenizer config keys"):
        tokenize.from_config({"tokenizer": "simple", "options": {"lowercase": True}})


def test_extract_key_field_handles_normalized_indexdef():
    indexdef = "CREATE INDEX idx ON public.products USING bm25 (id, description) WITH (key_field=id)"
    assert _extract_key_field(indexdef) == "id"


def test_extract_bm25_field_list_parses_tokenizer_casts():
    indexdef = (
        "CREATE INDEX idx ON public.products USING bm25 "
        "(id, ((description)::pdb.unicode_words(lowercase=true)), "
        "((category)::pdb.literal_normalized(alias=category_exact))) WITH (key_field=id)"
    )
    parts = _extract_bm25_field_list(indexdef)
    assert parts == [
        "id",
        "((description)::pdb.unicode_words(lowercase=true))",
        "((category)::pdb.literal_normalized(alias=category_exact))",
    ]
    assert _extract_field_name(parts[0]) == "id"
    assert _extract_field_name(parts[1]) == "description"
    assert _extract_field_name(parts[2]) == "category"
    assert _extract_alias(parts[2]) == "category_exact"


# ---------------------------------------------------------------------------
# _extract_tokenizer_name
# ---------------------------------------------------------------------------

def test_extract_tokenizer_name_unicode():
    expr = "(description::pdb.unicode_words('lowercase=true'))"
    assert _extract_tokenizer_name(expr) == "unicode_words"


def test_extract_tokenizer_name_literal_normalized():
    expr = "(category::pdb.literal_normalized('alias=category_exact'))"
    assert _extract_tokenizer_name(expr) == "literal_normalized"


def test_extract_tokenizer_name_no_options():
    expr = "(category::pdb.literal)"
    assert _extract_tokenizer_name(expr) == "literal"


def test_extract_tokenizer_name_plain_field_returns_none():
    assert _extract_tokenizer_name("id") is None


# ---------------------------------------------------------------------------
# IndexMeta.tokenizers population (unit-level, via describe helper stubs)
# ---------------------------------------------------------------------------

def test_index_meta_tokenizers_field_defaults_empty():
    meta = IndexMeta(
        index_name="idx",
        key_field="id",
        fields=("id",),
        aliases={},
    )
    assert meta.tokenizers == {}


def test_index_meta_tokenizers_stored():
    meta = IndexMeta(
        index_name="idx",
        key_field="id",
        fields=("id", "description"),
        aliases={},
        tokenizers={"description": ("unicode_words",)},
    )
    assert meta.tokenizers["description"] == ("unicode_words",)


# ---------------------------------------------------------------------------
# assert_indexed — error paths (no DB needed)
# ---------------------------------------------------------------------------

def test_assert_indexed_raises_when_column_has_no_table():
    from sqlalchemy import column, Integer
    bare_col = column("id", Integer)
    with pytest.raises(InvalidArgumentError, match="table-bound"):
        assert_indexed(None, bare_col)


def test_assert_indexed_raises_field_not_indexed(monkeypatch):
    """assert_indexed raises FieldNotIndexedError when describe() returns no matching index."""
    from paradedb.sqlalchemy import indexing as idx_module

    meta = IndexMeta(
        index_name="products_bm25_idx",
        key_field="id",
        fields=("id", "description"),
        aliases={},
    )
    monkeypatch.setattr(idx_module, "describe", lambda engine, table, schema=None: [meta])

    with pytest.raises(FieldNotIndexedError, match="'category'"):
        assert_indexed(None, products.c.category)


def test_assert_indexed_passes_when_field_found(monkeypatch):
    from paradedb.sqlalchemy import indexing as idx_module

    meta = IndexMeta(
        index_name="products_bm25_idx",
        key_field="id",
        fields=("id", "description", "category"),
        aliases={},
    )
    monkeypatch.setattr(idx_module, "describe", lambda engine, table, schema=None: [meta])

    # Should not raise
    assert_indexed(None, products.c.category)


def test_assert_indexed_tokenizer_match(monkeypatch):
    from paradedb.sqlalchemy import indexing as idx_module

    meta = IndexMeta(
        index_name="products_bm25_idx",
        key_field="id",
        fields=("id", "category"),
        aliases={},
        tokenizers={"category": ("literal",)},
    )
    monkeypatch.setattr(idx_module, "describe", lambda engine, table, schema=None: [meta])

    assert_indexed(None, products.c.category, tokenizer="literal")  # passes


def test_assert_indexed_tokenizer_mismatch_raises(monkeypatch):
    from paradedb.sqlalchemy import indexing as idx_module

    meta = IndexMeta(
        index_name="products_bm25_idx",
        key_field="id",
        fields=("id", "category"),
        aliases={},
        tokenizers={"category": ("unicode_words",)},
    )
    monkeypatch.setattr(idx_module, "describe", lambda engine, table, schema=None: [meta])

    with pytest.raises(FieldNotIndexedError, match="tokenizer 'literal'"):
        assert_indexed(None, products.c.category, tokenizer="literal")


def test_assert_indexed_passes_schema_override_to_describe(monkeypatch):
    from paradedb.sqlalchemy import indexing as idx_module

    meta = IndexMeta(
        index_name="products_bm25_idx",
        key_field="id",
        fields=("id", "category"),
        aliases={},
    )
    captured: dict[str, object] = {}

    def _describe(engine, table, schema=None):
        captured["schema"] = schema
        return [meta]

    monkeypatch.setattr(idx_module, "describe", _describe)
    assert_indexed(None, products.c.category, schema="analytics")
    assert captured["schema"] == "analytics"
