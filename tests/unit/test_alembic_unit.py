from __future__ import annotations

from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.render import render_op
from alembic.migration import MigrationContext

import paradedb.sqlalchemy.alembic as pdb_alembic


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
        fields=["id", "description"],
        key_field="id",
    )
    pdb_alembic._create_bm25_index_impl(ops, create_op)
    assert (
        ops.sql[-1]
        == 'CREATE INDEX "idx ""quoted""" ON "tbl ""quoted""" USING bm25 ("id", "description") WITH (key_field=\'id\')'
    )

    drop_op = pdb_alembic.DropBM25IndexOp(index_name='idx "quoted"', if_exists=True)
    pdb_alembic._drop_bm25_index_impl(ops, drop_op)
    assert ops.sql[-1] == 'DROP INDEX IF EXISTS "idx ""quoted"""'

    reindex_op = pdb_alembic.ReindexBM25Op(index_name='idx "quoted"', concurrently=True)
    pdb_alembic._reindex_bm25_impl(ops, reindex_op)
    assert ops.sql[-1] == 'REINDEX INDEX CONCURRENTLY "idx ""quoted"""'


def test_alembic_renderers_registered_and_emit_python():
    ctx = MigrationContext.configure(dialect_name="postgresql")
    autogen_ctx = AutogenContext(ctx)

    create_lines = render_op(
        autogen_ctx,
        pdb_alembic.CreateBM25IndexOp(
            index_name="products_bm25_idx",
            table_name="products",
            fields=["id", "description"],
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
