"""
Alembic configuration used to test generating and running migrations in test_alembic_commands_integration.py
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


def _configure_context(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_name=include_name,
        include_object=include_object,
        version_table_schema=version_table_schema,
    )


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
            _configure_context(connection)
            with context.begin_transaction():
                context.run_migrations()
        return

    _configure_context(connectable)
    with context.begin_transaction():
        context.run_migrations()


run_migrations_online()
