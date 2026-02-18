from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, Text, select, text
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.orm import Session

from conftest import assert_uses_paradedb_scan
from paradedb.sqlalchemy import search


pytestmark = pytest.mark.integration


def test_range_query_with_op_and_all_predicate(engine):
    metadata = MetaData()
    items = Table(
        "range_items",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("description", Text, nullable=False),
        Column("weight_range", INT4RANGE, nullable=False),
    )

    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS range_items_bm25_idx"))
        conn.execute(text("DROP TABLE IF EXISTS range_items"))
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE INDEX range_items_bm25_idx ON range_items USING bm25 (id, description, weight_range) WITH (key_field='id')"
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO range_items (id, description, weight_range)
                VALUES
                  (1, 'Ergonomic camera strap', int4range(1, 8)),
                  (2, 'Mechanical keyboard', int4range(8, 15)),
                  (3, 'Running shoes', int4range(3, 10))
                """
            )
        )

    try:
        with Session(engine) as session:
            stmt = (
                select(items.c.id)
                .where(items.c.weight_range.op("@>")(5))
                .where(search.all(items.c.id))
                .order_by(items.c.id)
            )
            assert_uses_paradedb_scan(session, stmt, index_name="range_items_bm25_idx")
            ids = list(session.scalars(stmt))
            assert ids == [1, 3]

            stmt_with_text = (
                select(items.c.id)
                .where(items.c.weight_range.op("@>")(5))
                .where(search.match_any(items.c.description, "running", "camera"))
                .order_by(items.c.id)
            )
            assert_uses_paradedb_scan(session, stmt_with_text, index_name="range_items_bm25_idx")
            ids_with_text = list(session.scalars(stmt_with_text))
            assert ids_with_text == [1, 3]
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP INDEX IF EXISTS range_items_bm25_idx"))
            conn.execute(text("DROP TABLE IF EXISTS range_items"))
