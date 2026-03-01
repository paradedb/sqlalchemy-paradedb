from __future__ import annotations

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.operations.ops import UpgradeOps
from sqlalchemy import Column, Integer, MetaData, Table, Text, text
from sqlalchemy.exc import SQLAlchemyError
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
    except SQLAlchemyError:
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


# ---------------------------------------------------------------------------
# Helper: assert BM25 queryable via raw SQL EXPLAIN
# ---------------------------------------------------------------------------

def _assert_bm25_queryable(conn, table_name, index_name, search_column, search_term):
    """Run EXPLAIN (FORMAT TEXT) on a BM25 query and assert ParadeDB Scan is used."""
    sql = (
        f"EXPLAIN (FORMAT TEXT) SELECT * FROM \"{table_name}\" "
        f"WHERE \"{search_column}\" @@@ '{search_term}'"
    )
    rows = conn.execute(text(sql)).fetchall()
    plan_text = "\n".join(str(row[0]) for row in rows)
    assert "Custom Scan" in plan_text, f"Expected Custom Scan in plan:\n{plan_text}"
    assert index_name in plan_text, f"Expected index {index_name} in plan:\n{plan_text}"


# ---------------------------------------------------------------------------
# 2a. Partial index with WHERE clause
# ---------------------------------------------------------------------------

_PARTIAL_TABLE = "alembic_partial_test"
_PARTIAL_IDX = "alembic_partial_bm25_idx"


def test_alembic_create_partial_index_with_where_clause(engine):
    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_PARTIAL_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_PARTIAL_TABLE}" CASCADE'))
        conn.execute(text(
            f'CREATE TABLE "{_PARTIAL_TABLE}" ('
            f'"id" int primary key, "description" text not null, "rating" int not null)'
        ))

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)
        op.create_bm25_index(
            _PARTIAL_IDX, _PARTIAL_TABLE, ["id", "description", "rating"],
            key_field="id", where="rating > 3",
        )

        # Verify pg_indexes.indexdef contains WHERE
        indexdef = conn.execute(text(
            "SELECT indexdef FROM pg_indexes WHERE indexname = :idx"
        ), {"idx": _PARTIAL_IDX}).scalar_one()
        assert "WHERE" in indexdef

        # Insert rows — some matching, some not
        conn.execute(text(
            f'INSERT INTO "{_PARTIAL_TABLE}" VALUES '
            f"(1, 'Excellent running shoes', 5), "
            f"(2, 'Decent running shoes', 3), "
            f"(3, 'Premium running gear', 4)"
        ))

        # BM25 query should only return rows matching the predicate
        result = conn.execute(text(
            f"SELECT id FROM \"{_PARTIAL_TABLE}\" "
            f"WHERE description @@@ 'running' AND rating > 3 ORDER BY id"
        )).fetchall()
        ids = [r[0] for r in result]
        assert 1 in ids
        assert 3 in ids
        assert 2 not in ids

    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_PARTIAL_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_PARTIAL_TABLE}" CASCADE'))


# ---------------------------------------------------------------------------
# 2b. Autogenerate detects missing partial index
# ---------------------------------------------------------------------------

def test_autogenerate_detects_missing_partial_index(engine):
    _setup_autogen_table(engine, with_index=False)
    try:
        m = MetaData()
        t = Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
        from sqlalchemy.schema import Index
        Index(
            _AG_IDX,
            BM25Field(t.c.id),
            BM25Field(t.c.description),
            postgresql_using="bm25",
            postgresql_with={"key_field": "id"},
            postgresql_where=t.c.id > 2,
        )

        upgrade_ops = _run_comparator(engine, m)
        create_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.CreateBM25IndexOp)]
        assert len(create_ops) == 1
        op = create_ops[0]
        assert op.index_name == _AG_IDX
        assert op.where is not None
        assert "2" in op.where
    finally:
        _teardown_autogen_table(engine)


# ---------------------------------------------------------------------------
# 2c. Autogenerate detects changed partial predicate
# ---------------------------------------------------------------------------

def test_autogenerate_detects_changed_partial_predicate(engine):
    _setup_autogen_table(engine, with_index=False)
    try:
        # Create index with WHERE (id > 2) in DB
        with engine.begin() as conn:
            conn.execute(text(
                f'CREATE INDEX "{_AG_IDX}" ON "{_AG_TABLE}" '
                f"USING bm25 (id, description) WITH (key_field='id') WHERE (id > 2)"
            ))

        # MetaData declares WHERE (id > 5)
        m = MetaData()
        t = Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
        from sqlalchemy.schema import Index
        Index(
            _AG_IDX,
            BM25Field(t.c.id),
            BM25Field(t.c.description),
            postgresql_using="bm25",
            postgresql_with={"key_field": "id"},
            postgresql_where=t.c.id > 5,
        )

        upgrade_ops = _run_comparator(engine, m)
        drop_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.DropBM25IndexOp) and op.index_name == _AG_IDX]
        create_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.CreateBM25IndexOp) and op.index_name == _AG_IDX]
        assert len(drop_ops) == 1, "Expected DropBM25IndexOp for predicate change"
        assert len(create_ops) == 1, "Expected CreateBM25IndexOp for predicate change"
        assert "5" in create_ops[0].where
    finally:
        _teardown_autogen_table(engine)


# ---------------------------------------------------------------------------
# 2d. Matching partial indexes should not emit drift
# ---------------------------------------------------------------------------

def test_autogenerate_no_op_when_partial_indexes_match(engine):
    _setup_autogen_table(engine, with_index=False)
    try:
        with engine.begin() as conn:
            conn.execute(text(
                f'CREATE INDEX "{_AG_IDX}" ON "{_AG_TABLE}" '
                f"USING bm25 (id, description) WITH (key_field='id') WHERE (id > 2)"
            ))

        m = MetaData()
        t = Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
        from sqlalchemy.schema import Index

        Index(
            _AG_IDX,
            BM25Field(t.c.id),
            BM25Field(t.c.description),
            postgresql_using="bm25",
            postgresql_with={"key_field": "id"},
            postgresql_where=t.c.id > 2,
        )

        upgrade_ops = _run_comparator(engine, m)
        our_ops = [
            op for op in upgrade_ops.ops
            if getattr(op, "index_name", None) == _AG_IDX
        ]
        assert not our_ops, f"Expected no ops for matching partial index, got: {our_ops}"
    finally:
        _teardown_autogen_table(engine)


# ---------------------------------------------------------------------------
# 2e. String-literal case drift in partial predicates should be detected
# ---------------------------------------------------------------------------

def test_autogenerate_detects_changed_partial_string_literal_case(engine):
    _setup_autogen_table(engine, with_index=False)
    try:
        with engine.begin() as conn:
            conn.execute(text(
                f'CREATE INDEX "{_AG_IDX}" ON "{_AG_TABLE}" '
                f"USING bm25 (id, description) WITH (key_field='id') "
                f"WHERE (description = 'ACTIVE')"
            ))

        m = MetaData()
        t = Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
        from sqlalchemy.schema import Index

        Index(
            _AG_IDX,
            BM25Field(t.c.id),
            BM25Field(t.c.description),
            postgresql_using="bm25",
            postgresql_with={"key_field": "id"},
            postgresql_where="description = 'active'::text",
        )

        upgrade_ops = _run_comparator(engine, m)
        drop_ops = [
            op for op in upgrade_ops.ops
            if isinstance(op, pdb_alembic.DropBM25IndexOp) and op.index_name == _AG_IDX
        ]
        create_ops = [
            op for op in upgrade_ops.ops
            if isinstance(op, pdb_alembic.CreateBM25IndexOp) and op.index_name == _AG_IDX
        ]
        assert len(drop_ops) == 1, "Expected DropBM25IndexOp for string-literal case change"
        assert len(create_ops) == 1, "Expected CreateBM25IndexOp for string-literal case change"
    finally:
        _teardown_autogen_table(engine)


# ---------------------------------------------------------------------------
# 2f. Autogenerate round-trip converges (no diff after applying ops)
# ---------------------------------------------------------------------------

def test_autogenerate_round_trip_converges(engine):
    _setup_autogen_table(engine, with_index=True)
    try:
        # First pass: metadata matches DB → no ops for our index
        upgrade_ops = _run_comparator(engine, _metadata_with_bm25())
        our_ops = [
            op for op in upgrade_ops.ops
            if (isinstance(op, pdb_alembic.CreateBM25IndexOp) and op.index_name == _AG_IDX)
            or (isinstance(op, pdb_alembic.DropBM25IndexOp) and op.index_name == _AG_IDX)
        ]
        assert not our_ops, f"Expected zero ops on convergence, got: {our_ops}"
    finally:
        _teardown_autogen_table(engine)


# ---------------------------------------------------------------------------
# 2g. Full lifecycle: create → query → reindex → query → drop
# ---------------------------------------------------------------------------

_LIFECYCLE_TABLE = "alembic_lifecycle_test"
_LIFECYCLE_IDX = "alembic_lifecycle_bm25_idx"


def test_alembic_create_reindex_drop_is_queryable(engine):
    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_LIFECYCLE_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_LIFECYCLE_TABLE}" CASCADE'))
        conn.execute(text(
            f'CREATE TABLE "{_LIFECYCLE_TABLE}" ('
            f'"id" int primary key, "description" text not null)'
        ))
        conn.execute(text(
            f"INSERT INTO \"{_LIFECYCLE_TABLE}\" VALUES "
            f"(1, 'Sleek running shoes'), (2, 'Wireless headphones'), (3, 'Trail running gear')"
        ))

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        # Create
        op.create_bm25_index(_LIFECYCLE_IDX, _LIFECYCLE_TABLE, ["id", "description"], key_field="id")
        _assert_bm25_queryable(conn, _LIFECYCLE_TABLE, _LIFECYCLE_IDX, "description", "running")

        # Reindex
        op.reindex_bm25(_LIFECYCLE_IDX)
        _assert_bm25_queryable(conn, _LIFECYCLE_TABLE, _LIFECYCLE_IDX, "description", "running")

        # Drop
        op.drop_bm25_index(_LIFECYCLE_IDX, if_exists=True)
        exists = conn.execute(text(
            "SELECT COUNT(*) FROM pg_indexes WHERE indexname = :idx"
        ), {"idx": _LIFECYCLE_IDX}).scalar_one()
        assert exists == 0

    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{_LIFECYCLE_TABLE}" CASCADE'))


# ---------------------------------------------------------------------------
# 2h. Reindex concurrently with AUTOCOMMIT
# ---------------------------------------------------------------------------

_CONC_TABLE = "alembic_conc_test"
_CONC_IDX = "alembic_conc_bm25_idx"


def test_alembic_reindex_concurrently_autocommit(engine):
    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_CONC_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_CONC_TABLE}" CASCADE'))
        conn.execute(text(
            f'CREATE TABLE "{_CONC_TABLE}" ("id" int primary key, "description" text not null)'
        ))
        conn.execute(text(
            f"INSERT INTO \"{_CONC_TABLE}\" VALUES (1, 'Test running shoes')"
        ))

    # Create index normally
    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)
        op.create_bm25_index(_CONC_IDX, _CONC_TABLE, ["id", "description"], key_field="id")

    # Reindex concurrently requires AUTOCOMMIT
    autocommit_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
    with autocommit_engine.connect() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)
        op.reindex_bm25(_CONC_IDX, concurrently=True)

    # Verify index still works
    with engine.connect() as conn:
        _assert_bm25_queryable(conn, _CONC_TABLE, _CONC_IDX, "description", "running")

    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_CONC_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_CONC_TABLE}" CASCADE'))


# ---------------------------------------------------------------------------
# 2i. Autogenerate detects changed key_field
# ---------------------------------------------------------------------------

def test_autogenerate_detects_changed_key_field(engine):
    _setup_autogen_table(engine, with_index=False)
    try:
        # DB has key_field='id'
        with engine.begin() as conn:
            conn.execute(text(
                f'CREATE INDEX "{_AG_IDX}" ON "{_AG_TABLE}" '
                f"USING bm25 (id, description) WITH (key_field='id')"
            ))

        # MetaData declares key_field='description' (different) but keeps the
        # expression list identical so only key_field drift is under test.
        m = MetaData()
        t = Table(_AG_TABLE, m, Column("id", Integer, primary_key=True), Column("description", Text))
        from sqlalchemy.schema import Index
        Index(
            _AG_IDX,
            BM25Field(t.c.id),
            BM25Field(t.c.description),
            postgresql_using="bm25",
            postgresql_with={"key_field": "description"},
        )

        upgrade_ops = _run_comparator(engine, m)
        drop_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.DropBM25IndexOp) and op.index_name == _AG_IDX]
        create_ops = [op for op in upgrade_ops.ops if isinstance(op, pdb_alembic.CreateBM25IndexOp) and op.index_name == _AG_IDX]
        assert len(drop_ops) == 1, "Expected DropBM25IndexOp for key_field change"
        assert len(create_ops) == 1, "Expected CreateBM25IndexOp for key_field change"
        assert create_ops[0].key_field == "description"
    finally:
        _teardown_autogen_table(engine)


# ---------------------------------------------------------------------------
# 2j. Expression index lifecycle (with tokenizer)
# ---------------------------------------------------------------------------

_EXPR_TABLE = "alembic_expr_test"
_EXPR_IDX = "alembic_expr_bm25_idx"


def test_alembic_expression_index_lifecycle(engine):
    if not _tokenizer_cast_supported(engine):
        pytest.skip("ParadeDB instance does not support tokenizer cast index syntax yet")

    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_EXPR_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_EXPR_TABLE}" CASCADE'))
        conn.execute(text(
            f'CREATE TABLE "{_EXPR_TABLE}" ("id" int primary key, "description" text not null)'
        ))

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)
        op.create_bm25_index(
            _EXPR_IDX, _EXPR_TABLE,
            ["id", "((description)::pdb.simple('alias=desc_simple,lowercase=true'))"],
            key_field="id",
        )

        # Verify index exists and indexdef contains the tokenizer expression
        indexdef = conn.execute(text(
            "SELECT indexdef FROM pg_indexes WHERE indexname = :idx"
        ), {"idx": _EXPR_IDX}).scalar_one()
        assert "pdb.simple" in indexdef
        assert "desc_simple" in indexdef

        op.drop_bm25_index(_EXPR_IDX, if_exists=True)
        exists = conn.execute(text(
            "SELECT COUNT(*) FROM pg_indexes WHERE indexname = :idx"
        ), {"idx": _EXPR_IDX}).scalar_one()
        assert exists == 0

    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{_EXPR_TABLE}" CASCADE'))


# ---------------------------------------------------------------------------
# 2k. Multi-tokenizer expression lifecycle
# ---------------------------------------------------------------------------

_MULTI_TABLE = "alembic_multi_tok_test"
_MULTI_IDX = "alembic_multi_tok_bm25_idx"


def test_alembic_multi_tokenizer_expression_lifecycle(engine):
    if not _tokenizer_cast_supported(engine):
        pytest.skip("ParadeDB instance does not support tokenizer cast index syntax yet")

    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_MULTI_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_MULTI_TABLE}" CASCADE'))
        conn.execute(text(
            f'CREATE TABLE "{_MULTI_TABLE}" ('
            f'"id" int primary key, "title" text not null, "body" text not null)'
        ))

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)
        op.create_bm25_index(
            _MULTI_IDX, _MULTI_TABLE,
            [
                "id",
                "((title)::pdb.simple('alias=title_simple,lowercase=true'))",
                "((body)::pdb.unicode_words('alias=body_unicode,lowercase=true'))",
            ],
            key_field="id",
        )

        # Verify index exists and indexdef contains both tokenizer expressions
        indexdef = conn.execute(text(
            "SELECT indexdef FROM pg_indexes WHERE indexname = :idx"
        ), {"idx": _MULTI_IDX}).scalar_one()
        assert "pdb.simple" in indexdef
        assert "title_simple" in indexdef
        assert "pdb.unicode_words" in indexdef
        assert "body_unicode" in indexdef

    with engine.begin() as conn:
        conn.execute(text(f'DROP INDEX IF EXISTS "{_MULTI_IDX}"'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{_MULTI_TABLE}" CASCADE'))
