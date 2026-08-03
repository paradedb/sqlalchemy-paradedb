from __future__ import annotations

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateIndex

from paradedb import Tokenizer
from paradedb.sqlalchemy.indexing import (
    ParadeDBField,
    IndexMeta,
    _extract_alias,
    _extract_paradedb_field_list,
    _extract_field_name,
    _extract_key_field,
    _extract_tokenizer_name,
    _is_paradedb_index,
    assert_indexed,
    validate_paradedb_index,
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
    assert tokenizer.unicode_words(
        options={"alias": "description_unicode", "lowercase": True, "stemmer": "english"}
    ).render() == ("pdb.unicode_words('alias=description_unicode','lowercase=true','stemmer=english')")
    assert tokenizer.simple(
        options={"alias": "description_simple", "lowercase": True, "stemmer": "english"}
    ).render() == ("pdb.simple('alias=description_simple','lowercase=true','stemmer=english')")
    assert tokenizer.whitespace(options={"alias": "description_whitespace", "positions": True}).render() == (
        "pdb.whitespace('alias=description_whitespace','positions=true')"
    )
    assert tokenizer.icu(options={"alias": "description_icu", "lowercase": True}).render() == (
        "pdb.icu('alias=description_icu','lowercase=true')"
    )
    assert tokenizer.chinese_compatible(options={"alias": "description_cjk"}).render() == (
        "pdb.chinese_compatible('alias=description_cjk')"
    )
    assert tokenizer.jieba(options={"alias": "description_jieba", "lowercase": True}).render() == (
        "pdb.jieba('alias=description_jieba','lowercase=true')"
    )
    assert tokenizer.literal(options={"alias": "category_literal"}).render() == "pdb.literal('alias=category_literal')"
    assert tokenizer.literal_normalized(options={"alias": "category_exact"}).render() == (
        "pdb.literal_normalized('alias=category_exact')"
    )
    assert tokenizer.ngram(3, 8, options={"alias": "description_ngram", "prefix_only": True}).render() == (
        "pdb.ngram(3,8,'alias=description_ngram','prefix_only=true')"
    )
    assert tokenizer.edge_ngram(3, 8, options={"alias": "description_ngram"}).render() == (
        "pdb.edge_ngram(3,8,'alias=description_ngram')"
    )
    assert tokenizer.lindera("japanese", options={"alias": "description_jp"}).render() == (
        "pdb.lindera('japanese','alias=description_jp')"
    )
    assert tokenizer.regex_pattern(r"(?i)\\bh\\w*", options={"alias": "description_regex"}).render() == (
        "pdb.regex_pattern('(?i)\\\\bh\\\\w*','alias=description_regex')"
    )
    assert tokenizer.source_code(options={"alias": "description_source_code", "ascii_folding": True}).render() == (
        "pdb.source_code('alias=description_source_code','ascii_folding=true')"
    )
    assert Tokenizer("source_code", options={"alias": "description_source_code", "ascii_folding": True}).render() == (
        "pdb.source_code('alias=description_source_code','ascii_folding=true')"
    )


def test_tokenizer_invalid_argument_types():
    with pytest.raises(InvalidArgumentError):
        tokenizer.whitespace(options={"invalid_type": None}).render()
    with pytest.raises(InvalidArgumentError):
        Tokenizer("ngram", positional_args=(None,)).render()


def test_paradedb_index_compile_with_tokenizers():
    idx = Index(
        "products_search_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            products.c.description,
            tokenizer=tokenizer.unicode_words(options={"lowercase": True, "stemmer": "english"}),
        ),
        ParadeDBField(products.c.category, tokenizer=tokenizer.literal_normalized(options={"alias": "category_exact"})),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_search_idx ON products USING paradedb (id, ((description)::pdb.unicode_words('lowercase=true','stemmer=english')), ((category)::pdb.literal_normalized('alias=category_exact'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_unicode_omits_none_options():
    idx = Index(
        "products_search_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(products.c.description, tokenizer=tokenizer.unicode_words(options={"lowercase": True})),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_search_idx ON products USING paradedb (id, ((description)::pdb.unicode_words('lowercase=true'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_with_structured_tokenizer_config():
    idx = Index(
        "products_structured_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            products.c.description,
            tokenizer=tokenizer.simple(
                options={"alias": "description_simple", "lowercase": True, "stemmer": "english"}
            ),
        ),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_structured_idx ON products USING paradedb (id, ((description)::pdb.simple('alias=description_simple','lowercase=true','stemmer=english'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_with_tokenizer_positional_and_named_args():
    idx = Index(
        "products_ngram_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            products.c.description,
            tokenizer=tokenizer.ngram(
                3, 8, options={"alias": "description_ngram", "prefix_only": True, "positions": True}
            ),
        ),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_ngram_idx ON products USING paradedb (id, ((description)::pdb.ngram(3,8,'alias=description_ngram','prefix_only=true','positions=true'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_lindera_wrapper():
    idx = Index(
        "products_lindera_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            products.c.description, tokenizer=tokenizer.lindera("japanese", options={"alias": "description_jp"})
        ),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_lindera_idx ON products USING paradedb (id, ((description)::pdb.lindera('japanese','alias=description_jp'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_regex_pattern_wrapper():
    idx = Index(
        "products_regex_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            products.c.description,
            tokenizer=tokenizer.regex_pattern(r"(?i)\\bh\\w*", options={"alias": "description_regex"}),
        ),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_regex_idx ON products USING paradedb (id, ((description)::pdb.regex_pattern('(?i)\\\\bh\\\\w*','alias=description_regex'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_json_key_with_tokenizer():
    idx = Index(
        "products_json_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            json_text(products.c.metadata, "color"),
            tokenizer=tokenizer.literal(options={"alias": "metadata_color"}),
        ),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_json_idx ON products USING paradedb (id, ((metadata ->> 'color')::pdb.literal('alias=metadata_color'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_multiple_json_keys():
    idx = Index(
        "products_json_multi_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            json_text(products.c.metadata, "color"),
            tokenizer=tokenizer.literal(options={"alias": "metadata_color"}),
        ),
        ParadeDBField(
            json_text(products.c.metadata, "location"),
            tokenizer=tokenizer.literal(options={"alias": "metadata_location"}),
        ),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )
    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_json_multi_idx ON products USING paradedb (id, ((metadata ->> 'color')::pdb.literal('alias=metadata_color')), ((metadata ->> 'location')::pdb.literal('alias=metadata_location'))) WITH (key_field = id)"""
    )


def test_paradedb_index_compile_non_text_expression_with_pdb_alias():
    idx = Index(
        "products_expr_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(products.c.description),
        ParadeDBField(pdb.alias(products.c.id + 1, "next_id")),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    assert (
        _sql(CreateIndex(idx).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        == """\
CREATE INDEX products_expr_idx ON products USING paradedb (id, description, ((id + 1)::pdb.alias('next_id'))) WITH (key_field = id)"""
    )


def test_paradedb_field_non_postgres_compile_raises():
    with pytest.raises(CompileError, match="ParadeDBField is only supported"):
        str(ParadeDBField(products.c.id).compile(dialect=sqlite.dialect()))


def test_is_paradedb_index_accepts_paradedb_and_bm25():
    for am in ("paradedb", "bm25"):
        idx = Index(
            f"products_{am}_recognition_idx",
            ParadeDBField(products.c.id),
            postgresql_using=am,
            postgresql_with={"key_field": "id"},
        )
        assert _is_paradedb_index(idx), am

    for am in ("gin", "gist", "btree", None):
        other = Index(f"products_{am or 'default'}_idx", products.c.description, postgresql_using=am)
        assert not _is_paradedb_index(other), am


def test_duplicate_alias_validation_raises():
    idx = Index(
        "products_alias_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(
            products.c.description, tokenizer=tokenizer.unicode_words(options={"alias": "description_alias"})
        ),
        ParadeDBField(products.c.category, tokenizer=tokenizer.literal(options={"alias": "description_alias"})),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="Duplicate tokenizer alias"):
        validate_paradedb_index(idx)


def test_key_field_validation_raises_when_missing():
    idx = Index(
        "products_missing_key_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(products.c.description),
        postgresql_using="paradedb",
    )

    with pytest.raises(ValueError, match="key_field"):
        validate_paradedb_index(idx)


def test_key_field_must_exist_in_fields():
    idx = Index(
        "products_bad_key_idx",
        ParadeDBField(products.c.id),
        ParadeDBField(products.c.description),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "missing"},
    )

    with pytest.raises(ValueError, match="must match one of the indexed"):
        validate_paradedb_index(idx)


def test_key_field_must_be_first_field():
    idx = Index(
        "products_key_not_first_idx",
        ParadeDBField(products.c.description),
        ParadeDBField(products.c.id),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="must be the first indexed ParadeDBField"):
        validate_paradedb_index(idx)


def test_key_field_must_be_untokenized():
    idx = Index(
        "products_key_tokenized_idx",
        ParadeDBField(products.c.id, tokenizer=tokenizer.literal(options={"alias": "id_alias"})),
        ParadeDBField(products.c.description),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    with pytest.raises(ValueError, match="must be untokenized"):
        validate_paradedb_index(idx)


def test_extract_key_field_handles_normalized_indexdef():
    indexdef = "CREATE INDEX idx ON public.products USING paradedb (id, description) WITH (key_field=id)"
    assert _extract_key_field(indexdef) == "id"


def test_extract_paradedb_field_list_parses_tokenizer_casts():
    indexdef = (
        "CREATE INDEX idx ON public.products USING paradedb "
        "(id, ((description)::pdb.unicode_words('lowercase=true')), "
        "((category)::pdb.literal_normalized('alias=category_exact'))) WITH (key_field=id)"
    )
    parts = _extract_paradedb_field_list(indexdef)
    assert parts == [
        "id",
        "((description)::pdb.unicode_words('lowercase=true'))",
        "((category)::pdb.literal_normalized('alias=category_exact'))",
    ]
    assert _extract_field_name(parts[0]) == "id"
    assert _extract_field_name(parts[1]) == "description"
    assert _extract_field_name(parts[2]) == "category"
    assert _extract_alias(parts[2]) == "category_exact"


def test_extract_paradedb_field_list_parses_legacy_bm25_indexdef():
    indexdef = "CREATE INDEX idx ON public.products USING bm25 (id, description) WITH (key_field=id)"
    assert _extract_paradedb_field_list(indexdef) == ["id", "description"]


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
        index_name="products_search_idx",
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
        index_name="products_search_idx",
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
        index_name="products_search_idx",
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
        index_name="products_search_idx",
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
        index_name="products_search_idx",
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
