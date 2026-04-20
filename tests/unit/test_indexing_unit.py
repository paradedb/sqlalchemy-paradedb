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
    validate_bm25_index,
)
from paradedb.sqlalchemy import tokenizer
from paradedb.sqlalchemy.errors import FieldNotIndexedError, InvalidArgumentError
from paradedb.sqlalchemy import pdb
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


def _sql(sql) -> str:
    return "\n".join(line.rstrip() for line in str(sql).split("\n")).strip()


def test_tokenizer_renderers_cover_public_wrappers():
    assert tokenizer.unicode(alias="description_unicode", lowercase=True, stemmer="english").render() == (
        "pdb.unicode_words('alias=description_unicode,lowercase=true,stemmer=english')"
    )
    assert tokenizer.simple(
        alias="description_simple", filters=["lowercase", "stemmer"], stemmer="english"
    ).render() == ("pdb.simple('alias=description_simple,lowercase=true,stemmer=english')")
    assert tokenizer.whitespace(alias="description_whitespace", named_args={"positions": True}).render() == (
        "pdb.whitespace('alias=description_whitespace,positions=true')"
    )
    assert tokenizer.icu(alias="description_icu", filters=["lowercase"]).render() == (
        "pdb.icu('alias=description_icu,lowercase=true')"
    )
    assert tokenizer.chinese_compatible(alias="description_cjk").render() == (
        "pdb.chinese_compatible('alias=description_cjk')"
    )
    assert tokenizer.jieba(alias="description_jieba", filters=["lowercase"]).render() == (
        "pdb.jieba('alias=description_jieba,lowercase=true')"
    )
    assert tokenizer.literal(alias="category_literal").render() == "pdb.literal('alias=category_literal')"
    assert tokenizer.literal_normalized(alias="category_exact").render() == (
        "pdb.literal_normalized('alias=category_exact')"
    )
    assert tokenizer.ngram(alias="description_ngram", min_gram=3, max_gram=8, prefix_only=True).render() == (
        "pdb.ngram(3,8,'alias=description_ngram,prefix_only=true')"
    )
    assert tokenizer.lindera("japanese", alias="description_jp").render() == (
        "pdb.lindera('japanese','alias=description_jp')"
    )
    assert tokenizer.regex_pattern(r"(?i)\\bh\\w*", alias="description_regex").render() == (
        "pdb.regex_pattern('(?i)\\\\bh\\\\w*','alias=description_regex')"
    )
    assert tokenizer.source_code(alias="description_source_code", named_args={"ascii_folding": True}).render() == (
        "pdb.source_code('alias=description_source_code,ascii_folding=true')"
    )


def test_bm25_index_compile_with_tokenizers():
    idx = Index(
        "products_bm25_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenizer.unicode(lowercase=True, stemmer="english")),
        BM25Field(products.c.category, tokenizer=tokenizer.literal_normalized(alias="category_exact")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_idx ON products USING bm25 (id, ((description)::pdb.unicode_words('lowercase=true,stemmer=english')), ((category)::pdb.literal_normalized('alias=category_exact'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_unicode_omits_none_options():
    idx = Index(
        "products_bm25_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenizer.unicode(lowercase=True)),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_idx ON products USING bm25 (id, ((description)::pdb.unicode_words('lowercase=true'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_with_structured_tokenizer_config():
    idx = Index(
        "products_bm25_structured_idx",
        BM25Field(products.c.id),
        BM25Field(
            products.c.description,
            tokenizer=tokenizer.simple(filters=["lowercase", "stemmer"], stemmer="english", alias="description_simple"),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_structured_idx ON products USING bm25 (id, ((description)::pdb.simple('alias=description_simple,lowercase=true,stemmer=english'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_with_tokenizer_positional_and_named_args():
    idx = Index(
        "products_bm25_ngram_idx",
        BM25Field(products.c.id),
        BM25Field(
            products.c.description,
            tokenizer=tokenizer.ngram(
                min_gram=3, max_gram=8, named_args={"prefix_only": True, "positions": True}, alias="description_ngram"
            ),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_ngram_idx ON products USING bm25 (id, ((description)::pdb.ngram(3,8,'alias=description_ngram,prefix_only=true,positions=true'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_lindera_wrapper():
    idx = Index(
        "products_bm25_lindera_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenizer.lindera("japanese", alias="description_jp")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_lindera_idx ON products USING bm25 (id, ((description)::pdb.lindera('japanese','alias=description_jp'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_regex_pattern_wrapper():
    idx = Index(
        "products_bm25_regex_idx",
        BM25Field(products.c.id),
        BM25Field(
            products.c.description, tokenizer=tokenizer.regex_pattern(r"(?i)\\bh\\w*", alias="description_regex")
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_regex_idx ON products USING bm25 (id, ((description)::pdb.regex_pattern('(?i)\\\\bh\\\\w*','alias=description_regex'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_json_key_with_tokenizer():
    idx = Index(
        "products_bm25_json_idx",
        BM25Field(products.c.id),
        BM25Field(
            json_text(products.c.metadata, "color"),
            tokenizer=tokenizer.literal(alias="metadata_color"),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_json_idx ON products USING bm25 (id, ((metadata ->> 'color')::pdb.literal('alias=metadata_color'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_multiple_json_keys():
    idx = Index(
        "products_bm25_json_multi_idx",
        BM25Field(products.c.id),
        BM25Field(
            json_text(products.c.metadata, "color"),
            tokenizer=tokenizer.literal(alias="metadata_color"),
        ),
        BM25Field(
            json_text(products.c.metadata, "location"),
            tokenizer=tokenizer.literal(alias="metadata_location"),
        ),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_json_multi_idx ON products USING bm25 (id, ((metadata ->> 'color')::pdb.literal('alias=metadata_color')), ((metadata ->> 'location')::pdb.literal('alias=metadata_location'))) WITH (key_field = id)"""
    )


def test_bm25_index_compile_non_text_expression_with_pdb_alias():
    idx = Index(
        "products_bm25_expr_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description),
        BM25Field(pdb.alias(products.c.id + 1, "next_id")),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_bm25_expr_idx ON products USING bm25 (id, description, ((id + 1)::pdb.alias('next_id'))) WITH (key_field = id)"""
    )


def test_bm25_field_non_postgres_compile_raises():
    with pytest.raises(CompileError, match="BM25Field is only supported"):
        str(BM25Field(products.c.id).compile(dialect=sqlite.dialect()))


def test_duplicate_alias_validation_raises():
    idx = Index(
        "products_bm25_alias_idx",
        BM25Field(products.c.id),
        BM25Field(products.c.description, tokenizer=tokenizer.unicode(alias="description_alias")),
        BM25Field(products.c.category, tokenizer=tokenizer.literal(alias="description_alias")),
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
        BM25Field(products.c.id, tokenizer=tokenizer.literal(alias="id_alias")),
        BM25Field(products.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="must be untokenized"):
        validate_bm25_index(idx)


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


def test_extract_field_name_from_json_key_tokenizer_cast():
    expr = "((metadata ->> 'color')::pdb.literal('alias=metadata_color'))"
    assert _extract_field_name(expr) == "metadata"


def test_extract_field_name_from_qualified_tokenizer_cast():
    expr = "((public.products.description)::pdb.unicode_words('lowercase=true'))"
    assert _extract_field_name(expr) == "description"


def test_extract_field_name_from_quoted_identifier():
    expr = "((\"Display Name\")::pdb.literal('alias=display_name'))"
    assert _extract_field_name(expr) == "Display Name"


def test_extract_field_name_from_escaped_quoted_identifier():
    expr = '(("Display ""Name""")::pdb.literal(\'alias=display_name\'))'
    assert _extract_field_name(expr) == 'Display "Name"'


def test_extract_field_name_from_qualified_json_key_tokenizer_cast():
    expr = "(((public.products.metadata ->> 'color'::text))::pdb.literal(2))"
    assert _extract_field_name(expr) == "metadata"


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
