from __future__ import annotations

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.operations.ops import UpgradeOps
from sqlalchemy import Column, Integer, MetaData, Table, Text, text
from unittest.mock import MagicMock

import paradedb.sqlalchemy.alembic as pdb_alembic  # noqa: F401  Ensure op registration
from paradedb.sqlalchemy.indexing import BM25Field, tokenize


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helper: run the BM25 autogenerate comparator against a real DB connection
# ---------------------------------------------------------------------------

def _run_comparator(engine, metadata, schemas=None):
    """Return the UpgradeOps produced by the BM25 autogenerate comparator."""
    if schemas is None:
        schemas = {None}
    with engine.connect() as conn:
        ctx = MagicMock()
        ctx.connection = conn
        ctx.metadata = metadata
        upgrade_ops = UpgradeOps([])
        pdb_alembic._compare_bm25_indexes(ctx, upgrade_ops, schemas)
    return upgrade_ops


def test_alembic_create_reindex_drop_with_quoted_identifiers(engine):
    table_name = 'alembic quoted products'
    index_name = 'alembic quoted idx'

    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{index_name}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        conn.execute(text(f'CREATE TABLE "{table_name}" ("id" int primary key, "description" text not null)'))

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

        exists_after = conn.execute(
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
        assert exists_after == 0

    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))


def test_alembic_create_reindex_drop_with_schema(engine):
    schema = "alembic_ops_schema"
    table_name = "alembic_products"
    index_name = "alembic_products_idx"

    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        conn.execute(text(f'CREATE TABLE "{schema}"."{table_name}" ("id" int primary key, "description" text not null)'))

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)
        conn.execute(text("SET LOCAL search_path TO public"))

        op.create_bm25_index(
            index_name,
            table_name,
            ["id", "description"],
            key_field="id",
            table_schema=schema,
        )

        exists = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pg_indexes
                WHERE schemaname = :schema
                  AND tablename = :table_name
                  AND indexname = :index_name
                """
            ),
            {"schema": schema, "table_name": table_name, "index_name": index_name},
        ).scalar_one()
        assert exists == 1

        op.reindex_bm25(index_name, schema=schema)
        op.drop_bm25_index(index_name, schema=schema, if_exists=True)

        exists_after = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pg_indexes
                WHERE schemaname = :schema
                  AND tablename = :table_name
                  AND indexname = :index_name
                """
            ),
            {"schema": schema, "table_name": table_name, "index_name": index_name},
        ).scalar_one()
        assert exists_after == 0

    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


# ---------------------------------------------------------------------------
# Autogenerate comparator integration tests
# ---------------------------------------------------------------------------

_AG_TABLE = "autogen_test"
_AG_IDX = "autogen_test_bm25_idx"


def _setup_autogen_table(engine, *, with_index: bool = False):
    """Create a clean autogen_test table (and optionally a BM25 index) in the DB."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_AG_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_AG_TABLE}" CASCADE'))
        conn.execute(text(f'CREATE TABLE "{_AG_TABLE}" (id int primary key, description text not null)'))
        if with_index:
            conn.execute(
                text(
                    f'CREATE INDEX "{_AG_IDX}" ON "{_AG_TABLE}" '
                    f"USING bm25 (id, description) WITH (key_field='id')"
                )
            )


def _teardown_autogen_table(engine):
    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_AG_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_AG_TABLE}" CASCADE'))


def _metadata_with_bm25() -> MetaData:
    """MetaData that defines autogen_test with a BM25 index."""
    m = MetaData()
    t = Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
    from sqlalchemy.schema import Index
    Index(
        _AG_IDX,
        BM25Field(t.c.id),
        BM25Field(t.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    return m


def _metadata_without_bm25() -> MetaData:
    """MetaData that defines autogen_test WITHOUT any BM25 index."""
    m = MetaData()
    Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
    return m


def test_autogenerate_detects_missing_index(engine):
    """MetaData has BM25 index but DB does not → CreateBM25IndexOp emitted."""
    _setup_autogen_table(engine, with_index=False)
    try:
        upgrade_ops = _run_comparator(engine, _metadata_with_bm25())

        create_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.CreateBM25IndexOp)]
        assert len(create_ops) == 1
        op = create_ops[0]
        assert op.index_name == _AG_IDX
        assert op.table_name == _AG_TABLE
        assert op.key_field == "id"
        assert "id" in op.expressions
        assert "description" in op.expressions
    finally:
        _teardown_autogen_table(engine)


def test_autogenerate_detects_extra_index(engine):
    """DB has BM25 index but MetaData does not → DropBM25IndexOp emitted."""
    _setup_autogen_table(engine, with_index=True)
    try:
        upgrade_ops = _run_comparator(engine, _metadata_without_bm25())

        drop_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.DropBM25IndexOp)]
        assert any(op.index_name == _AG_IDX for op in drop_ops)
    finally:
        _teardown_autogen_table(engine)


def test_autogenerate_no_op_when_indexes_match(engine):
    """DB and MetaData have identical BM25 index → no create/drop ops for that index."""
    _setup_autogen_table(engine, with_index=True)
    try:
        upgrade_ops = _run_comparator(engine, _metadata_with_bm25())

        # Filter to only ops for our specific test index; the shared engine fixture's
        # products_bm25_idx may appear as "extra" since our MetaData only knows autogen_test.
        create_ops = [
            op for op in upgrade_ops.ops
            if isinstance(op, pdb_alembic.CreateBM25IndexOp) and op.index_name == _AG_IDX
        ]
        drop_ops = [
            op for op in upgrade_ops.ops
            if isinstance(op, pdb_alembic.DropBM25IndexOp) and op.index_name == _AG_IDX
        ]
        assert not create_ops
        assert not drop_ops
    finally:
        _teardown_autogen_table(engine)


def test_autogenerate_detects_changed_fields(engine):
    """BM25 index in DB has different fields vs MetaData → Drop + Create emitted."""
    _setup_autogen_table(engine, with_index=False)
    try:
        # DB index only covers 'id'
        with engine.begin() as conn:
            conn.execute(
                text(f'CREATE INDEX "{_AG_IDX}" ON "{_AG_TABLE}" USING bm25 (id) WITH (key_field=\'id\')')
            )

        # MetaData index covers 'id' and 'description'
        upgrade_ops = _run_comparator(engine, _metadata_with_bm25())

        drop_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.DropBM25IndexOp)]
        create_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.CreateBM25IndexOp)]
        assert any(op.index_name == _AG_IDX for op in drop_ops), "Expected DropBM25IndexOp"
        assert any(op.index_name == _AG_IDX for op in create_ops), "Expected CreateBM25IndexOp"
    finally:
        _teardown_autogen_table(engine)


def _tokenizer_cast_supported(engine) -> bool:
    table_name = "autogen_tok_support"
    index_name = "autogen_tok_support_idx"
    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP INDEX IF EXISTS "{index_name}"'))
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
            conn.execute(text(f'CREATE TABLE "{table_name}" (id int primary key, description text not null)'))
            conn.execute(
                text(
                    f'CREATE INDEX "{index_name}" ON "{table_name}" '
                    "USING bm25 (id, (description::pdb.unicode_words('lowercase=true'))) "
                    "WITH (key_field='id')"
                )
            )
        return True
    except Exception:
        return False
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP INDEX IF EXISTS "{index_name}"'))
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


def _metadata_with_tokenized_bm25() -> MetaData:
    m = MetaData()
    t = Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
    from sqlalchemy.schema import Index

    Index(
        _AG_IDX,
        BM25Field(t.c.id),
        BM25Field(t.c.description, tokenizer=tokenize.simple(alias="description_simple", filters=["lowercase"])),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    return m


def test_autogenerate_detects_changed_tokenizer_expression(engine):
    if not _tokenizer_cast_supported(engine):
        pytest.skip("ParadeDB instance does not support tokenizer cast index syntax yet")

    _setup_autogen_table(engine, with_index=False)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f'CREATE INDEX "{_AG_IDX}" ON "{_AG_TABLE}" '
                    "USING bm25 (id, description) WITH (key_field='id')"
                )
            )

        upgrade_ops = _run_comparator(engine, _metadata_with_tokenized_bm25())

        drop_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.DropBM25IndexOp)]
        create_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.CreateBM25IndexOp)]
        assert any(op.index_name == _AG_IDX for op in drop_ops), "Expected DropBM25IndexOp"
        create = next(op for op in create_ops if op.index_name == _AG_IDX)
        assert any("pdb.simple" in expr for expr in create.expressions)
        assert any("alias=description_simple" in expr for expr in create.expressions)
    finally:
        _teardown_autogen_table(engine)


_AG_SCHEMA = "autogen_schema"
_AG_SCHEMA_TABLE = "autogen_schema_test"
_AG_SCHEMA_IDX = "autogen_schema_test_bm25_idx"


def _setup_autogen_schema_table(engine, *, with_index: bool = False):
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{_AG_SCHEMA}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{_AG_SCHEMA}"'))
        conn.execute(
            text(
                f'CREATE TABLE "{_AG_SCHEMA}"."{_AG_SCHEMA_TABLE}" '
                '(id int primary key, description text not null)'
            )
        )
        if with_index:
            conn.execute(
                text(
                    f'CREATE INDEX "{_AG_SCHEMA_IDX}" ON "{_AG_SCHEMA}"."{_AG_SCHEMA_TABLE}" '
                    "USING bm25 (id, description) WITH (key_field='id')"
                )
            )


def _metadata_with_bm25_in_schema() -> MetaData:
    m = MetaData()
    t = Table(
        _AG_SCHEMA_TABLE,
        m,
        Column("id", Integer, primary_key=True),
        Column("description", Text),
        schema=_AG_SCHEMA,
    )
    from sqlalchemy.schema import Index

    Index(
        _AG_SCHEMA_IDX,
        BM25Field(t.c.id),
        BM25Field(t.c.description),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )
    return m


def test_autogenerate_emits_schema_on_create_for_non_default_schema(engine):
    _setup_autogen_schema_table(engine, with_index=False)
    try:
        upgrade_ops = _run_comparator(engine, _metadata_with_bm25_in_schema(), schemas={_AG_SCHEMA})
        create_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.CreateBM25IndexOp)]
        op = next(o for o in create_ops if o.index_name == _AG_SCHEMA_IDX)
        assert op.table_schema == _AG_SCHEMA
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{_AG_SCHEMA}" CASCADE'))


def test_autogenerate_emits_schema_on_drop_for_non_default_schema(engine):
    _setup_autogen_schema_table(engine, with_index=True)
    try:
        metadata = MetaData()
        upgrade_ops = _run_comparator(engine, metadata, schemas={_AG_SCHEMA})
        drop_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.DropBM25IndexOp)]
        op = next(o for o in drop_ops if o.index_name == _AG_SCHEMA_IDX)
        assert op.schema == _AG_SCHEMA
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{_AG_SCHEMA}" CASCADE'))
