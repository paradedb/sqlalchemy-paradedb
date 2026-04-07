from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Boolean, Date, DateTime, Index, Integer, Text, Time, text
from sqlalchemy.dialects.postgresql import INT4RANGE, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import paradedb.sqlalchemy.alembic  # noqa: F401  Ensure Alembic op registration
from paradedb.sqlalchemy import indexing


class Base(DeclarativeBase):
    pass


pytestmark = pytest.mark.integration


def _make_alembic_config(tmp_path: Path, db_url: str, metadata) -> Config:
    script_location = tmp_path / "alembic"
    versions_dir = script_location / "versions"
    versions_dir.mkdir(parents=True)

    env_py = script_location / "env.py"
    env_py.write_text(
        """
from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

import paradedb.sqlalchemy.alembic  # noqa: F401

config = context.config
target_metadata = config.attributes["target_metadata"]
target_table_keys = set(target_metadata.tables)
target_schemas = {table.schema for table in target_metadata.tables.values() if table.schema is not None}
version_table_schema = next(iter(target_schemas), None)


def include_name(name, type_, parent_names):
    if type_ == "schema":
        return name in target_schemas
    if type_ == "table":
        schema_name = parent_names.get("schema_name")
        if schema_name not in target_schemas:
            return False
        key = ".".join(part for part in (schema_name, name) if part)
        return key in target_table_keys
    return True


def include_object(object_, name, type_, reflected, compare_to):
    if type_ == "table":
        table = object_ if hasattr(object_, "schema") else compare_to
        if table is None:
            return False
        key = ".".join(part for part in (table.schema, table.name) if part)
        return key in target_table_keys
    return True


def run_migrations_offline() -> None:
    raise RuntimeError("offline migrations are not used in this test")


def run_migrations_online() -> None:
    connectable = config.attributes.get("connection")
    if connectable is None:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )

    if hasattr(connectable, "connect"):
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                include_schemas=True,
                include_name=include_name,
                include_object=include_object,
                version_table_schema=version_table_schema,
            )
            with context.begin_transaction():
                context.run_migrations()
    else:
        context.configure(
            connection=connectable,
            target_metadata=target_metadata,
            include_schemas=True,
            include_name=include_name,
            include_object=include_object,
            version_table_schema=version_table_schema,
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
""".strip()
        + "\n",
        encoding="utf-8",
    )

    script_template = script_location / "script.py.mako"
    script_template.write_text(
        """\"\"\"${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

\"\"\"
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
""",
        encoding="utf-8",
    )

    config = Config()
    config.set_main_option("script_location", str(script_location))
    config.set_main_option("sqlalchemy.url", db_url)
    config.attributes["target_metadata"] = metadata
    return config


def _index_count(engine) -> int:
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM pg_indexes
                  WHERE tablename = 'mock_items'
                  AND indexname = 'search_idx'
                """
            )
        ).scalar_one()


def test_alembic_command_autogenerate_upgrade_and_downgrade_bm25_index(engine, db_url, tmp_path):
    suffix = uuid4().hex[:8]
    schema_name = f"alembic_cmd_{suffix}"

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

    config = _make_alembic_config(tmp_path, db_url, Base.metadata)

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
        assert _index_count(engine) == 1

        command.downgrade(config, "base")
        assert _index_count(engine) == 0
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
