from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import Boolean, Date, DateTime, Index, Integer, Text, Time, text
from sqlalchemy.dialects.postgresql import INT4RANGE, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import paradedb.sqlalchemy.alembic  # noqa: F401  Ensure Alembic op registration
from paradedb.sqlalchemy import indexing


pytestmark = pytest.mark.integration


def _index_count(engine, *, schema_name: str) -> int:
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pg_indexes
                  WHERE schemaname = :schema_name
                  AND tablename = 'mock_items'
                  AND indexname = 'search_idx'
                """
            ),
            {"schema_name": schema_name},
        ).scalar_one()


def _index_definition(engine, *, schema_name: str) -> str:
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                SELECT indexdef
                FROM pg_indexes
                  WHERE schemaname = :schema_name
                  AND tablename = 'mock_items'
                  AND indexname = 'search_idx'
                """
            ),
            {"schema_name": schema_name},
        ).scalar_one()


def _make_mock_item_model(schema_name: str):
    class Base(DeclarativeBase):
        pass

    class MockItem(Base):
        __tablename__ = "mock_items"
        __table_args__ = {"schema": schema_name}

        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        description: Mapped[str | None] = mapped_column(Text, nullable=True)
        rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
        category: Mapped[str | None] = mapped_column(Text, nullable=True)
        in_stock: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
        metadata_: Mapped[Any] = mapped_column("metadata", JSONB, nullable=True)
        created_at: Mapped[Any] = mapped_column(DateTime, nullable=True)
        last_updated_date: Mapped[Any] = mapped_column(Date, nullable=True)
        latest_available_time: Mapped[Any] = mapped_column(Time, nullable=True)
        weight_range: Mapped[Any] = mapped_column(INT4RANGE, nullable=True)

    return Base, MockItem


def test_alembic_command_autogenerate_upgrade_and_downgrade_bm25_index(engine, alembic_config_factory):
    suffix = uuid4().hex[:8]
    schema_name = f"alembic_cmd_{suffix}"
    Base, MockItem = _make_mock_item_model(schema_name)

    Index(
        "search_idx",
        indexing.BM25Field(MockItem.id),
        indexing.BM25Field(MockItem.description),
        indexing.BM25Field(MockItem.category),
        indexing.BM25Field(MockItem.rating),
        indexing.BM25Field(MockItem.in_stock),
        indexing.BM25Field(MockItem.metadata_),
        indexing.BM25Field(MockItem.created_at),
        indexing.BM25Field(MockItem.last_updated_date),
        indexing.BM25Field(MockItem.latest_available_time),
        indexing.BM25Field(MockItem.weight_range),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    config = alembic_config_factory(Base.metadata)

    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
            conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            conn.execute(
                text(
                    f"CALL paradedb.create_bm25_test_table(schema_name => '{schema_name}', table_name => 'mock_items')"
                )
            )

        command.revision(config, message="add bm25 index", autogenerate=True, rev_id=f"rev_{suffix}")

        command.upgrade(config, "head")
        assert _index_count(engine, schema_name=schema_name) == 1

        command.downgrade(config, "base")
        assert _index_count(engine, schema_name=schema_name) == 0
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


def test_alembic_command_autogenerate_changed_bm25_index(engine, alembic_config_factory):
    suffix = uuid4().hex[:8]
    schema_name = f"alembic_cmd_changed_{suffix}"
    Base, MockItem = _make_mock_item_model(schema_name)

    Index(
        "search_idx",
        indexing.BM25Field(MockItem.id),
        indexing.BM25Field(MockItem.description),
        indexing.BM25Field(MockItem.category),
        indexing.BM25Field(MockItem.rating),
        indexing.BM25Field(MockItem.in_stock),
        indexing.BM25Field(MockItem.metadata_),
        indexing.BM25Field(MockItem.created_at),
        indexing.BM25Field(MockItem.last_updated_date),
        indexing.BM25Field(MockItem.latest_available_time),
        indexing.BM25Field(MockItem.weight_range),
        postgresql_using="bm25",
        postgresql_with={"key_field": "id"},
    )

    config = alembic_config_factory(Base.metadata)

    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
            conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            conn.execute(
                text(
                    f"CALL paradedb.create_bm25_test_table(schema_name => '{schema_name}', table_name => 'mock_items')"
                )
            )
            conn.execute(
                text(
                    f'CREATE INDEX "search_idx" ON "{schema_name}"."mock_items" '
                    "USING bm25 (id, description) WITH (key_field='id')"
                )
            )

        original_indexdef = _index_definition(engine, schema_name=schema_name)
        assert "id, description" in original_indexdef
        assert "category" not in original_indexdef

        command.revision(config, message="update bm25 index", autogenerate=True, rev_id=f"rev_changed_{suffix}")

        command.upgrade(config, "head")
        upgraded_indexdef = _index_definition(engine, schema_name=schema_name)
        assert "category" in upgraded_indexdef
        assert "weight_range" in upgraded_indexdef

        command.downgrade(config, "base")
        downgraded_indexdef = _index_definition(engine, schema_name=schema_name)
        assert "id, description" in downgraded_indexdef
        assert "category" not in downgraded_indexdef
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
