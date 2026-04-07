from __future__ import annotations

import pytest
from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.render import render_op
from alembic.migration import MigrationContext
from alembic.operations.ops import CreateIndexOp, DowngradeOps, DropIndexOp, ModifyTableOps, UpgradeOps
from sqlalchemy import Column, Integer, MetaData, Table, Text

import paradedb.sqlalchemy.alembic as pdb_alembic
from paradedb.sqlalchemy.indexing import BM25Field


class DummyOps:
    def __init__(self):
        self.sql: list[str] = []

    def execute(self, sql: str) -> None:
        self.sql.append(sql)


def test_create_drop_reindex_sql_generation():
    ops = DummyOps()

    create_op = pdb_alembic.CreateBM25IndexOp(
        index_name='idx "quoted"',
        table_name='tbl "quoted"',
        expressions=["id", "description"],
        key_field="id",
    )
    pdb_alembic._create_bm25_index_impl(ops, create_op)
    assert (
        ops.sql[-1]
        == 'CREATE INDEX "idx ""quoted""" ON "tbl ""quoted""" USING bm25 (id, description) WITH (key_field=\'id\')'
    )

    drop_op = pdb_alembic.DropBM25IndexOp(index_name='idx "quoted"', if_exists=True)
    pdb_alembic._drop_bm25_index_impl(ops, drop_op)
    assert ops.sql[-1] == 'DROP INDEX IF EXISTS "idx ""quoted"""'

    reindex_op = pdb_alembic.ReindexBM25Op(index_name='idx "quoted"', concurrently=True)
    pdb_alembic._reindex_bm25_impl(ops, reindex_op)
    assert ops.sql[-1] == 'REINDEX INDEX CONCURRENTLY "idx ""quoted"""'


def test_create_sql_generation_preserves_tokenizer_expression():
    ops = DummyOps()
    create_op = pdb_alembic.CreateBM25IndexOp(
        index_name="products_bm25_idx",
        table_name="products",
        expressions=["id", "((description)::pdb.simple('alias=description_simple,lowercase=true'))"],
        key_field="id",
    )
    pdb_alembic._create_bm25_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "products_bm25_idx" ON "products" '
        "USING bm25 (id, ((description)::pdb.simple('alias=description_simple,lowercase=true'))) "
        "WITH (key_field='id')"
    )


def test_create_drop_reindex_sql_generation_with_schema():
    ops = DummyOps()
    create_op = pdb_alembic.CreateBM25IndexOp(
        index_name="products_bm25_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
        table_schema="analytics",
    )
    pdb_alembic._create_bm25_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "products_bm25_idx" ON "analytics"."products" '
        "USING bm25 (id, description) WITH (key_field='id')"
    )

    drop_op = pdb_alembic.DropBM25IndexOp(index_name="products_bm25_idx", if_exists=True, schema="analytics")
    pdb_alembic._drop_bm25_index_impl(ops, drop_op)
    assert ops.sql[-1] == 'DROP INDEX IF EXISTS "analytics"."products_bm25_idx"'

    reindex_op = pdb_alembic.ReindexBM25Op(index_name="products_bm25_idx", concurrently=True, schema="analytics")
    pdb_alembic._reindex_bm25_impl(ops, reindex_op)
    assert ops.sql[-1] == 'REINDEX INDEX CONCURRENTLY "analytics"."products_bm25_idx"'


def test_create_bm25_index_rejects_removed_index_schema_kwarg():
    with pytest.raises(TypeError, match="index_schema"):
        pdb_alembic.CreateBM25IndexOp.create_bm25_index(
            object(),
            "products_bm25_idx",
            "products",
            ["id", "description"],
            key_field="id",
            index_schema="analytics",
        )


def test_create_bm25_index_reverse_returns_drop_op():
    create_op = pdb_alembic.CreateBM25IndexOp(
        index_name="products_bm25_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
        table_schema="analytics",
    )

    reversed_op = create_op.reverse()

    assert isinstance(reversed_op, pdb_alembic.DropBM25IndexOp)
    assert reversed_op.index_name == "products_bm25_idx"
    assert reversed_op.schema == "analytics"
    assert reversed_op.if_exists is True


def test_upgrade_ops_reverse_into_handles_bm25_create_op():
    upgrade_ops = UpgradeOps(
        [
            pdb_alembic.CreateBM25IndexOp(
                index_name="products_bm25_idx",
                table_name="products",
                expressions=["id", "description"],
                key_field="id",
                table_schema="analytics",
            )
        ]
    )

    downgrade_ops = upgrade_ops.reverse_into(DowngradeOps([]))

    assert len(downgrade_ops.ops) == 1
    reversed_op = downgrade_ops.ops[0]
    assert isinstance(reversed_op, pdb_alembic.DropBM25IndexOp)
    assert reversed_op.index_name == "products_bm25_idx"
    assert reversed_op.schema == "analytics"
    assert reversed_op.if_exists is True


def test_alembic_renderers_registered_and_emit_python():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    create_lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateBM25IndexOp(
            index_name="products_bm25_idx",
            table_name="products",
            expressions=["id", "description"],
            key_field="id",
        ),
    )
    assert create_lines == [
        "op.create_bm25_index('products_bm25_idx', 'products', ['id', 'description'], key_field='id')"
    ]

    drop_lines = render_op(
        autogen_ctx,
        pdb_alembic.DropBM25IndexOp(index_name="products_bm25_idx", if_exists=False),
    )
    assert drop_lines == ["op.drop_bm25_index('products_bm25_idx', if_exists=False)"]

    reindex_lines = render_op(
        autogen_ctx,
        pdb_alembic.ReindexBM25Op(index_name="products_bm25_idx", concurrently=True),
    )
    assert reindex_lines == ["op.reindex_bm25('products_bm25_idx', concurrently=True)"]

    create_lines_with_schema = render_op(
        autogen_ctx,
        pdb_alembic.CreateBM25IndexOp(
            index_name="products_bm25_idx",
            table_name="products",
            expressions=["id", "description"],
            key_field="id",
            table_schema="analytics",
        ),
    )
    assert create_lines_with_schema == [
        "op.create_bm25_index('products_bm25_idx', 'products', ['id', 'description'], key_field='id', table_schema='analytics')"
    ]


# ---------------------------------------------------------------------------
# Autogenerate comparator helpers — unit tests (no DB required)
# ---------------------------------------------------------------------------


def _make_metadata_with_bm25() -> tuple[MetaData, object]:
    """Return (metadata, bm25_index) with a BM25 and a non-BM25 index."""
    from sqlalchemy.schema import Index

    m = MetaData()
    t = Table("products", m, Column("id", Integer), Column("description", Text))
    bm25_idx = Index(
        "products_bm25_idx",
        BM25Field(t.c.id),
        BM25Field(t.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    # A regular (non-BM25) index on the same table
    Index("products_desc_idx", t.c.description)
    return m, bm25_idx


def test_autogen_meta_indexes_finds_bm25_only():
    m, bm25_idx = _make_metadata_with_bm25()
    result = pdb_alembic._autogen_bm25_meta_indexes(m, {"public"}, default_schema="public")

    assert ("public", "products_bm25_idx") in result
    # Regular index must not appear
    assert ("public", "products_desc_idx") not in result


def test_autogen_meta_indexes_schema_filter():
    """Indexes belonging to a non-target schema are excluded."""
    from sqlalchemy.schema import Index

    m = MetaData()
    t = Table("things", m, Column("id", Integer), Column("body", Text), schema="other")
    Index(
        "things_bm25_idx",
        BM25Field(t.c.id),
        BM25Field(t.c.body),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    # Only looking at schema "public" — the "other" table's index must not appear
    result = pdb_alembic._autogen_bm25_meta_indexes(m, {"public"}, default_schema="public")
    assert ("other", "things_bm25_idx") not in result

    # When we look at "other", it should appear
    result2 = pdb_alembic._autogen_bm25_meta_indexes(m, {"other"}, default_schema="public")
    assert ("other", "things_bm25_idx") in result2


def test_autogen_meta_indexes_uses_explicit_default_schema_for_unschematized_tables():
    from sqlalchemy.schema import Index

    m = MetaData()
    t = Table("products", m, Column("id", Integer), Column("description", Text))
    Index(
        "products_bm25_idx",
        BM25Field(t.c.id),
        BM25Field(t.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    result_public = pdb_alembic._autogen_bm25_meta_indexes(m, {"public", "other"}, default_schema="public")
    assert ("public", "products_bm25_idx") in result_public

    result_other = pdb_alembic._autogen_bm25_meta_indexes(m, {"public", "other"}, default_schema="other")
    assert ("other", "products_bm25_idx") in result_other


def test_suppress_standard_bm25_ops_removes_from_modify_table_ops():
    """Ops for BM25 indexes inside ModifyTableOps are removed; non-BM25 ops survive."""
    m = MetaData()
    t = Table("products", m, Column("id", Integer), Column("description", Text))

    bm25_idx = CreateIndexOp("products_bm25_idx", "products", [t.c.id])
    regular_idx = CreateIndexOp("products_desc_idx", "products", [t.c.description])
    drop_bm25 = DropIndexOp("products_bm25_idx", "products")

    modify_ops = ModifyTableOps("products", [bm25_idx, regular_idx, drop_bm25], schema=None)
    upgrade_ops = UpgradeOps([modify_ops])

    pdb_alembic._suppress_standard_bm25_ops(upgrade_ops, {"products_bm25_idx"})

    # ModifyTableOps container is still there
    assert len(upgrade_ops.ops) == 1
    remaining = upgrade_ops.ops[0].ops
    # Only the regular index op survives
    assert len(remaining) == 1
    assert remaining[0].index_name == "products_desc_idx"


def test_suppress_standard_bm25_ops_removes_top_level():
    """Top-level CreateIndexOp/DropIndexOp for BM25 indexes are also removed."""
    m = MetaData()
    t = Table("products", m, Column("id", Integer))

    bm25_create = CreateIndexOp("bm25_idx", "products", [t.c.id])
    regular_create = CreateIndexOp("regular_idx", "products", [t.c.id])

    upgrade_ops = UpgradeOps([bm25_create, regular_create])
    pdb_alembic._suppress_standard_bm25_ops(upgrade_ops, {"bm25_idx"})

    assert len(upgrade_ops.ops) == 1
    assert upgrade_ops.ops[0].index_name == "regular_idx"


def test_suppress_standard_bm25_ops_noop_when_no_bm25():
    """When there are no BM25 indexes to suppress, ops are unchanged."""
    m = MetaData()
    t = Table("products", m, Column("id", Integer))

    regular_create = CreateIndexOp("regular_idx", "products", [t.c.id])
    upgrade_ops = UpgradeOps([regular_create])

    pdb_alembic._suppress_standard_bm25_ops(upgrade_ops, set())
    assert len(upgrade_ops.ops) == 1


def test_normalize_bm25_expression_keeps_dotted_literal_content():
    expr = "(description)::pdb.regex_pattern('run.*')"
    normalized = pdb_alembic._normalize_bm25_expression(expr)
    assert normalized == "(description)::pdb.regex_pattern('run.*')"


def test_normalize_bm25_expression_strips_relation_qualifiers_only():
    expr = '"public"."products"."description"::pdb.simple(\'alias=description_simple\')'
    normalized = pdb_alembic._normalize_bm25_expression(expr)
    assert normalized == "description::pdb.simple('alias=description_simple')"


# ---------------------------------------------------------------------------
# WHERE clause (partial index) support
# ---------------------------------------------------------------------------


def test_create_sql_generation_with_where_clause():
    ops = DummyOps()
    create_op = pdb_alembic.CreateBM25IndexOp(
        index_name="products_bm25_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
        where="rating > 3",
    )
    pdb_alembic._create_bm25_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "products_bm25_idx" ON "products" '
        "USING bm25 (id, description) WITH (key_field='id') WHERE rating > 3"
    )


def test_create_sql_generation_without_where_clause():
    """When where is None, no WHERE suffix is appended."""
    ops = DummyOps()
    create_op = pdb_alembic.CreateBM25IndexOp(
        index_name="products_bm25_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
    )
    pdb_alembic._create_bm25_index_impl(ops, create_op)
    assert (
        ops.sql[-1]
        == 'CREATE INDEX "products_bm25_idx" ON "products" USING bm25 (id, description) WITH (key_field=\'id\')'
    )


def test_renderer_emits_where_kwarg():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateBM25IndexOp(
            index_name="products_bm25_idx",
            table_name="products",
            expressions=["id", "description"],
            key_field="id",
            where="rating > 3",
        ),
    )
    assert len(lines) == 1
    assert "where='rating > 3'" in lines[0]


def test_renderer_omits_where_when_none():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateBM25IndexOp(
            index_name="products_bm25_idx",
            table_name="products",
            expressions=["id", "description"],
            key_field="id",
        ),
    )
    assert "where=" not in lines[0]


def test_normalize_where_clause():
    assert pdb_alembic._normalize_where(None) is None
    assert pdb_alembic._normalize_where("rating > 3") == "rating > 3"
    assert pdb_alembic._normalize_where('  "rating"  >  3  ') == "rating > 3"
    assert pdb_alembic._normalize_where("RATING > 3") == "rating > 3"


def test_extract_where_clause():
    from paradedb.sqlalchemy.indexing import _extract_where_clause

    indexdef = (
        "CREATE INDEX products_bm25_idx ON public.products "
        "USING bm25 (id, description) WITH (key_field='id') WHERE (rating > 3)"
    )
    assert _extract_where_clause(indexdef) == "rating > 3"

    indexdef_no_where = (
        "CREATE INDEX products_bm25_idx ON public.products USING bm25 (id, description) WITH (key_field='id')"
    )
    assert _extract_where_clause(indexdef_no_where) is None
