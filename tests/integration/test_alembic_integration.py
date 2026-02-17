from __future__ import annotations

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

import paradedb.sqlalchemy.alembic  # noqa: F401  Ensure op registration


pytestmark = pytest.mark.integration


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
