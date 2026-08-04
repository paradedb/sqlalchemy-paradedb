from __future__ import annotations

import pytest
from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.render import render_op
from alembic.migration import MigrationContext
from alembic.operations.ops import CreateIndexOp, DowngradeOps, DropIndexOp, ModifyTableOps, UpgradeOps
from sqlalchemy import Column, Integer, MetaData, Table, Text

import paradedb.sqlalchemy.alembic as pdb_alembic
from paradedb.sqlalchemy.indexing import ParadeDBField, VectorField, VectorIndexOptions
from paradedb.sqlalchemy.vector import Vector


class DummyOps:
    def __init__(self):
        self.sql: list[str] = []

    def execute(self, sql: str) -> None:
        self.sql.append(sql)


def test_create_drop_reindex_sql_generation():
    ops = DummyOps()

    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name='idx "quoted"',
        table_name='tbl "quoted"',
        expressions=["id", "description"],
        key_field="id",
    )
    pdb_alembic._create_paradedb_index_impl(ops, create_op)
    assert (
        ops.sql[-1]
        == 'CREATE INDEX "idx ""quoted""" ON "tbl ""quoted""" USING paradedb (id, description) WITH (key_field=\'id\')'
    )

    drop_op = pdb_alembic.DropParadeDBIndexOp(index_name='idx "quoted"', if_exists=True)
    pdb_alembic._drop_paradedb_index_impl(ops, drop_op)
    assert ops.sql[-1] == 'DROP INDEX IF EXISTS "idx ""quoted"""'

    reindex_op = pdb_alembic.ReindexParadeDBOp(index_name='idx "quoted"', concurrently=True)
    pdb_alembic._reindex_paradedb_impl(ops, reindex_op)
    assert ops.sql[-1] == 'REINDEX INDEX CONCURRENTLY "idx ""quoted"""'


def test_create_sql_generation_preserves_tokenizer_expression():
    ops = DummyOps()
    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name="products_search_idx",
        table_name="products",
        expressions=["id", "((description)::pdb.simple('alias=description_simple,lowercase=true'))"],
        key_field="id",
    )
    pdb_alembic._create_paradedb_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "products_search_idx" ON "products" '
        "USING paradedb (id, ((description)::pdb.simple('alias=description_simple,lowercase=true'))) "
        "WITH (key_field='id')"
    )


def test_create_drop_reindex_sql_generation_with_schema():
    ops = DummyOps()
    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name="products_search_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
        table_schema="analytics",
    )
    pdb_alembic._create_paradedb_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "products_search_idx" ON "analytics"."products" '
        "USING paradedb (id, description) WITH (key_field='id')"
    )

    drop_op = pdb_alembic.DropParadeDBIndexOp(index_name="products_search_idx", if_exists=True, schema="analytics")
    pdb_alembic._drop_paradedb_index_impl(ops, drop_op)
    assert ops.sql[-1] == 'DROP INDEX IF EXISTS "analytics"."products_search_idx"'

    reindex_op = pdb_alembic.ReindexParadeDBOp(index_name="products_search_idx", concurrently=True, schema="analytics")
    pdb_alembic._reindex_paradedb_impl(ops, reindex_op)
    assert ops.sql[-1] == 'REINDEX INDEX CONCURRENTLY "analytics"."products_search_idx"'


def test_create_paradedb_index_rejects_removed_index_schema_kwarg():
    with pytest.raises(TypeError, match="index_schema"):
        pdb_alembic.CreateParadeDBIndexOp.create_paradedb_index(
            object(),
            "products_search_idx",
            "products",
            ["id", "description"],
            key_field="id",
            index_schema="analytics",
        )


def test_create_paradedb_index_reverse_returns_drop_op():
    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name="products_search_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
        table_schema="analytics",
    )

    reversed_op = create_op.reverse()

    assert isinstance(reversed_op, pdb_alembic.DropParadeDBIndexOp)
    assert reversed_op.index_name == "products_search_idx"
    assert reversed_op.schema == "analytics"
    assert reversed_op.if_exists is True


def test_drop_paradedb_index_reverse_returns_create_op_when_metadata_present():
    drop_op = pdb_alembic.DropParadeDBIndexOp(
        index_name="products_search_idx",
        if_exists=True,
        schema="analytics",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
        where="rating > 3",
    )

    reversed_op = drop_op.reverse()

    assert isinstance(reversed_op, pdb_alembic.CreateParadeDBIndexOp)
    assert reversed_op.index_name == "products_search_idx"
    assert reversed_op.table_name == "products"
    assert reversed_op.expressions == ["id", "description"]
    assert reversed_op.key_field == "id"
    assert reversed_op.table_schema == "analytics"
    assert reversed_op.where == "rating > 3"


def test_drop_paradedb_index_reverse_raises_without_recreate_metadata():
    drop_op = pdb_alembic.DropParadeDBIndexOp(index_name="products_search_idx", if_exists=True, schema="analytics")

    with pytest.raises(NotImplementedError, match="requires recreate metadata"):
        drop_op.reverse()


def test_upgrade_ops_reverse_into_handles_paradedb_create_op():
    upgrade_ops = UpgradeOps(
        [
            pdb_alembic.CreateParadeDBIndexOp(
                index_name="products_search_idx",
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
    assert isinstance(reversed_op, pdb_alembic.DropParadeDBIndexOp)
    assert reversed_op.index_name == "products_search_idx"
    assert reversed_op.schema == "analytics"
    assert reversed_op.if_exists is True


def test_upgrade_ops_reverse_into_handles_paradedb_drop_op_with_recreate_metadata():
    upgrade_ops = UpgradeOps(
        [
            pdb_alembic.DropParadeDBIndexOp(
                index_name="products_search_idx",
                if_exists=True,
                schema="analytics",
                table_name="products",
                expressions=["id", "description"],
                key_field="id",
                where="rating > 3",
            )
        ]
    )

    downgrade_ops = upgrade_ops.reverse_into(DowngradeOps([]))

    assert len(downgrade_ops.ops) == 1
    reversed_op = downgrade_ops.ops[0]
    assert isinstance(reversed_op, pdb_alembic.CreateParadeDBIndexOp)
    assert reversed_op.index_name == "products_search_idx"
    assert reversed_op.table_name == "products"
    assert reversed_op.expressions == ["id", "description"]
    assert reversed_op.key_field == "id"
    assert reversed_op.table_schema == "analytics"
    assert reversed_op.where == "rating > 3"


def test_alembic_renderers_registered_and_emit_python():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    create_lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateParadeDBIndexOp(
            index_name="products_search_idx",
            table_name="products",
            expressions=["id", "description"],
            key_field="id",
        ),
    )
    assert create_lines == [
        "op.create_paradedb_index('products_search_idx', 'products', ['id', 'description'], key_field='id')"
    ]

    drop_lines = render_op(
        autogen_ctx,
        pdb_alembic.DropParadeDBIndexOp(index_name="products_search_idx", if_exists=False),
    )
    assert drop_lines == ["op.drop_paradedb_index('products_search_idx', if_exists=False)"]

    drop_lines_with_recreate = render_op(
        autogen_ctx,
        pdb_alembic.DropParadeDBIndexOp(
            index_name="products_search_idx",
            if_exists=False,
            schema="analytics",
            table_name="products",
            expressions=["id", "description"],
            key_field="id",
            where="rating > 3",
        ),
    )
    assert drop_lines_with_recreate == [
        "op.drop_paradedb_index('products_search_idx', if_exists=False, schema='analytics', table_name='products', expressions=['id', 'description'], key_field='id', where='rating > 3')"
    ]

    reindex_lines = render_op(
        autogen_ctx,
        pdb_alembic.ReindexParadeDBOp(index_name="products_search_idx", concurrently=True),
    )
    assert reindex_lines == ["op.reindex_paradedb('products_search_idx', concurrently=True)"]

    create_lines_with_schema = render_op(
        autogen_ctx,
        pdb_alembic.CreateParadeDBIndexOp(
            index_name="products_search_idx",
            table_name="products",
            expressions=["id", "description"],
            key_field="id",
            table_schema="analytics",
        ),
    )
    assert create_lines_with_schema == [
        "op.create_paradedb_index('products_search_idx', 'products', ['id', 'description'], key_field='id', table_schema='analytics')"
    ]


# ---------------------------------------------------------------------------
# Autogenerate comparator helpers — unit tests (no DB required)
# ---------------------------------------------------------------------------


def _make_metadata_with_paradedb_index() -> tuple[MetaData, object]:
    """Return (metadata, paradedb_index) with a ParadeDB and a non-ParadeDB index."""
    from sqlalchemy.schema import Index

    m = MetaData()
    t = Table("products", m, Column("id", Integer), Column("description", Text))
    paradedb_idx = Index(
        "products_search_idx",
        ParadeDBField(t.c.id),
        ParadeDBField(t.c.description),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )
    # A regular (non-ParadeDB) index on the same table
    Index("products_desc_idx", t.c.description)
    return m, paradedb_idx


def test_autogen_meta_indexes_finds_paradedb_only():
    m, paradedb_idx = _make_metadata_with_paradedb_index()
    result = pdb_alembic._autogen_paradedb_meta_indexes(m, {"public"}, default_schema="public")

    assert ("public", "products_search_idx") in result
    # Regular index must not appear
    assert ("public", "products_desc_idx") not in result


def test_autogen_meta_indexes_schema_filter():
    """Indexes belonging to a non-target schema are excluded."""
    from sqlalchemy.schema import Index

    m = MetaData()
    t = Table("things", m, Column("id", Integer), Column("body", Text), schema="other")
    Index(
        "things_search_idx",
        ParadeDBField(t.c.id),
        ParadeDBField(t.c.body),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    # Only looking at schema "public" — the "other" table's index must not appear
    result = pdb_alembic._autogen_paradedb_meta_indexes(m, {"public"}, default_schema="public")
    assert ("other", "things_search_idx") not in result

    # When we look at "other", it should appear
    result2 = pdb_alembic._autogen_paradedb_meta_indexes(m, {"other"}, default_schema="public")
    assert ("other", "things_search_idx") in result2


def test_autogen_meta_indexes_uses_explicit_default_schema_for_unschematized_tables():
    from sqlalchemy.schema import Index

    m = MetaData()
    t = Table("products", m, Column("id", Integer), Column("description", Text))
    Index(
        "products_search_idx",
        ParadeDBField(t.c.id),
        ParadeDBField(t.c.description),
        postgresql_using="paradedb",
        postgresql_with={"key_field": "id"},
    )

    result_public = pdb_alembic._autogen_paradedb_meta_indexes(m, {"public", "other"}, default_schema="public")
    assert ("public", "products_search_idx") in result_public

    result_other = pdb_alembic._autogen_paradedb_meta_indexes(m, {"public", "other"}, default_schema="other")
    assert ("other", "products_search_idx") in result_other


def test_suppress_standard_paradedb_ops_removes_from_modify_table_ops():
    """Ops for ParadeDB indexes inside ModifyTableOps are removed; non-ParadeDB ops survive."""
    m = MetaData()
    t = Table("products", m, Column("id", Integer), Column("description", Text))

    paradedb_idx = CreateIndexOp("products_search_idx", "products", [t.c.id])
    regular_idx = CreateIndexOp("products_desc_idx", "products", [t.c.description])
    drop_paradedb = DropIndexOp("products_search_idx", "products")

    modify_ops = ModifyTableOps("products", [paradedb_idx, regular_idx, drop_paradedb], schema=None)
    upgrade_ops = UpgradeOps([modify_ops])

    pdb_alembic._suppress_standard_paradedb_ops(upgrade_ops, {"products_search_idx"})

    # ModifyTableOps container is still there
    assert len(upgrade_ops.ops) == 1
    remaining = upgrade_ops.ops[0].ops
    # Only the regular index op survives
    assert len(remaining) == 1
    assert remaining[0].index_name == "products_desc_idx"


def test_suppress_standard_paradedb_ops_removes_top_level():
    """Top-level CreateIndexOp/DropIndexOp for ParadeDB indexes are also removed."""
    m = MetaData()
    t = Table("products", m, Column("id", Integer))

    paradedb_create = CreateIndexOp("search_idx", "products", [t.c.id])
    regular_create = CreateIndexOp("regular_idx", "products", [t.c.id])

    upgrade_ops = UpgradeOps([paradedb_create, regular_create])
    pdb_alembic._suppress_standard_paradedb_ops(upgrade_ops, {"search_idx"})

    assert len(upgrade_ops.ops) == 1
    assert upgrade_ops.ops[0].index_name == "regular_idx"


def test_suppress_standard_paradedb_ops_noop_when_no_paradedb_indexes():
    """When there are no ParadeDB indexes to suppress, ops are unchanged."""
    m = MetaData()
    t = Table("products", m, Column("id", Integer))

    regular_create = CreateIndexOp("regular_idx", "products", [t.c.id])
    upgrade_ops = UpgradeOps([regular_create])

    pdb_alembic._suppress_standard_paradedb_ops(upgrade_ops, set())
    assert len(upgrade_ops.ops) == 1


def test_normalize_paradedb_expression_keeps_dotted_literal_content():
    expr = "(description)::pdb.regex_pattern('run.*')"
    normalized = pdb_alembic._normalize_paradedb_expression(expr)
    assert normalized == "(description)::pdb.regex_pattern('run.*')"


def test_normalize_paradedb_expression_strips_relation_qualifiers_only():
    expr = '"public"."products"."description"::pdb.simple(\'alias=description_simple\')'
    normalized = pdb_alembic._normalize_paradedb_expression(expr)
    assert normalized == "description::pdb.simple('alias=description_simple')"


def test_strip_relation_qualifiers_strips_schema_and_table_prefixes():
    assert pdb_alembic._strip_relation_qualifiers("analytics.id", "products", "analytics") == "id"
    assert pdb_alembic._strip_relation_qualifiers('"analytics"."products"."description"', "products", "analytics") == (
        '"description"'
    )


def test_strip_relation_qualifiers_avoids_substring_false_positive():
    expr = "featured_products.description = 'analytics.id'"
    assert pdb_alembic._strip_relation_qualifiers(expr, "products", "analytics") == expr

    expr = '"analytics_schema".products_table."description"'
    assert pdb_alembic._strip_relation_qualifiers(expr, "products", "analytics") == expr


# ---------------------------------------------------------------------------
# WHERE clause (partial index) support
# ---------------------------------------------------------------------------


def test_create_sql_generation_with_where_clause():
    ops = DummyOps()
    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name="products_search_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
        where="rating > 3",
    )
    pdb_alembic._create_paradedb_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "products_search_idx" ON "products" '
        "USING paradedb (id, description) WITH (key_field='id') WHERE rating > 3"
    )


def test_create_sql_generation_without_where_clause():
    """When where is None, no WHERE suffix is appended."""
    ops = DummyOps()
    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name="products_search_idx",
        table_name="products",
        expressions=["id", "description"],
        key_field="id",
    )
    pdb_alembic._create_paradedb_index_impl(ops, create_op)
    assert (
        ops.sql[-1]
        == 'CREATE INDEX "products_search_idx" ON "products" USING paradedb (id, description) WITH (key_field=\'id\')'
    )


def test_renderer_emits_where_kwarg():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateParadeDBIndexOp(
            index_name="products_search_idx",
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
        pdb_alembic.CreateParadeDBIndexOp(
            index_name="products_search_idx",
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
        "CREATE INDEX products_search_idx ON public.products "
        "USING paradedb (id, description) WITH (key_field='id') WHERE (rating > 3)"
    )
    assert _extract_where_clause(indexdef) == "rating > 3"

    indexdef_no_where = (
        "CREATE INDEX products_search_idx ON public.products USING paradedb (id, description) WITH (key_field='id')"
    )
    assert _extract_where_clause(indexdef_no_where) is None


def test_render_vector_field_expression():
    items = Table(
        "items",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("embedding", Vector(3)),
    )
    rendered = pdb_alembic._render_paradedb_expression(VectorField(items.c.embedding, metric="cosine"))
    assert pdb_alembic._strip_relation_qualifiers(rendered, "items", None) == "embedding vector_cosine_ops"


def test_normalize_paradedb_expression_strips_default_vector_opclass():
    assert pdb_alembic._normalize_paradedb_expression("embedding vector_l2_ops") == "embedding"
    assert pdb_alembic._normalize_paradedb_expression("embedding") == "embedding"
    assert pdb_alembic._normalize_paradedb_expression("embedding vector_cosine_ops") == "embeddingvector_cosine_ops"


# ---------------------------------------------------------------------------
# Vector index WITH options support
# ---------------------------------------------------------------------------


def test_create_sql_generation_with_all_vector_options():
    ops = DummyOps()
    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name="items_search_idx",
        table_name="items",
        expressions=["id", "embedding vector_cosine_ops"],
        key_field="id",
        with_options=VectorIndexOptions(centroid_ratio=0.01, training_samples_per_centroid=32, cluster_replication=1),
    )
    pdb_alembic._create_paradedb_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "items_search_idx" ON "items" '
        "USING paradedb (id, embedding vector_cosine_ops) "
        "WITH (key_field='id', centroid_ratio=0.01, training_samples_per_centroid=32, cluster_replication=1)"
    )


def test_create_sql_generation_with_single_option():
    ops = DummyOps()
    create_op = pdb_alembic.CreateParadeDBIndexOp(
        index_name="items_search_idx",
        table_name="items",
        expressions=["id", "embedding vector_l2_ops"],
        key_field="id",
        with_options=VectorIndexOptions(centroid_ratio=0.5),
    )
    pdb_alembic._create_paradedb_index_impl(ops, create_op)
    assert ops.sql[-1] == (
        'CREATE INDEX "items_search_idx" ON "items" '
        "USING paradedb (id, embedding vector_l2_ops) WITH (key_field='id', centroid_ratio=0.5)"
    )


def test_ops_reject_plain_dict_with_options():
    with pytest.raises(TypeError, match="VectorIndexOptions"):
        pdb_alembic.CreateParadeDBIndexOp(
            index_name="items_search_idx",
            table_name="items",
            expressions=["id"],
            key_field="id",
            with_options={"centroid_ratio": 0.01},
        )
    with pytest.raises(TypeError, match="VectorIndexOptions"):
        pdb_alembic.DropParadeDBIndexOp(
            index_name="items_search_idx",
            with_options={"centroid_ratio": 0.01},
        )


def test_renderer_emits_with_options_kwarg():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateParadeDBIndexOp(
            index_name="items_search_idx",
            table_name="items",
            expressions=["id", "embedding vector_cosine_ops"],
            key_field="id",
            with_options=VectorIndexOptions(
                centroid_ratio=0.01, training_samples_per_centroid=32, cluster_replication=1
            ),
        ),
    )
    assert lines == [
        "op.create_paradedb_index('items_search_idx', 'items', ['id', 'embedding vector_cosine_ops'], key_field='id', "
        "with_options=VectorIndexOptions(centroid_ratio=0.01, training_samples_per_centroid=32, cluster_replication=1))"
    ]
    assert "from paradedb.sqlalchemy import VectorIndexOptions" in autogen_ctx.imports


def test_renderer_omits_with_options_when_none():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateParadeDBIndexOp(
            index_name="items_search_idx",
            table_name="items",
            expressions=["id"],
            key_field="id",
        ),
    )
    assert lines == ["op.create_paradedb_index('items_search_idx', 'items', ['id'], key_field='id')"]


def test_drop_renderer_emits_with_options_kwarg():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    lines = render_op(
        autogen_ctx,
        pdb_alembic.DropParadeDBIndexOp(
            index_name="items_search_idx",
            if_exists=True,
            table_name="items",
            expressions=["id"],
            key_field="id",
            with_options=VectorIndexOptions(centroid_ratio=0.01),
        ),
    )
    assert lines == [
        "op.drop_paradedb_index('items_search_idx', if_exists=True, table_name='items', expressions=['id'], "
        "key_field='id', with_options=VectorIndexOptions(centroid_ratio=0.01))"
    ]
    assert "from paradedb.sqlalchemy import VectorIndexOptions" in autogen_ctx.imports


def test_drop_paradedb_index_reverse_carries_with_options():
    drop_op = pdb_alembic.DropParadeDBIndexOp(
        index_name="items_search_idx",
        if_exists=True,
        table_name="items",
        expressions=["id", "embedding vector_cosine_ops"],
        key_field="id",
        with_options=VectorIndexOptions(centroid_ratio=0.01, cluster_replication=1),
    )

    reversed_op = drop_op.reverse()

    assert isinstance(reversed_op, pdb_alembic.CreateParadeDBIndexOp)
    assert reversed_op.with_options == VectorIndexOptions(centroid_ratio=0.01, cluster_replication=1)


def test_parse_index_reloptions_excludes_key_field():
    reloptions = ["key_field=id", "centroid_ratio=0.01", "training_samples_per_centroid=32"]
    assert pdb_alembic._parse_index_reloptions(reloptions) == {
        "centroid_ratio": "0.01",
        "training_samples_per_centroid": "32",
    }
    assert pdb_alembic._parse_index_reloptions(None) == {}


def test_with_option_values_equal_tolerates_real_normalization():
    assert pdb_alembic._with_option_values_equal("0.0099999998", 0.01)
    assert pdb_alembic._with_option_values_equal("0.01", "0.01")
    assert pdb_alembic._with_option_values_equal("32", 32)
    assert not pdb_alembic._with_option_values_equal("0.02", 0.01)


def test_with_options_changed():
    assert not pdb_alembic._with_options_changed({}, {})
    assert not pdb_alembic._with_options_changed(
        {"centroid_ratio": "0.0099999998", "cluster_replication": "1"},
        {"centroid_ratio": 0.01, "cluster_replication": 1},
    )
    assert pdb_alembic._with_options_changed({}, {"centroid_ratio": 0.01})
    assert pdb_alembic._with_options_changed({"centroid_ratio": "0.01"}, {})
    assert pdb_alembic._with_options_changed({"centroid_ratio": "0.01"}, {"centroid_ratio": 0.02})
