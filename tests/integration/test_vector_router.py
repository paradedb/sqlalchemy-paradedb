import pytest
from sqlalchemy import Column, Index, Integer, MetaData, Table, text
from sqlalchemy.schema import CreateIndex
from sqlalchemy.dialects import postgresql

from paradedb import IndexOptions, ParadeDBField, Vector, VectorField
from paradedb.sqlalchemy.alembic import _to_vector_index_options
from paradedb.sqlalchemy.errors import InvalidIndexOptionError


@pytest.mark.parametrize("router", ["graph", "ivf"])
def test_vector_router_index_and_alembic_round_trip(engine, router):
    metadata = MetaData()
    items = Table("router_items", metadata, Column("id", Integer), Column("embedding", Vector(64)))
    options = IndexOptions(vector_router=router)
    Index(
        "router_idx",
        ParadeDBField(items.c.id),
        VectorField(items.c.embedding),
        postgresql_using="paradedb",
        postgresql_with=dict(options),
    )
    metadata.create_all(engine)
    try:
        with engine.connect() as conn:
            values = conn.execute(
                text("SELECT reloptions FROM pg_class WHERE oid = 'router_idx'::regclass")
            ).scalar_one()
        assert f"vector_router={router}" in values
        assert _to_vector_index_options(dict(option.split("=", 1) for option in values)) == options
    finally:
        metadata.drop_all(engine)


def test_vector_router_default_and_validation():
    items = Table("router_items", MetaData(), Column("id", Integer))
    index = Index(
        "router_idx", ParadeDBField(items.c.id), postgresql_using="paradedb", postgresql_with=dict(IndexOptions())
    )
    assert "vector_router" not in str(CreateIndex(index).compile(dialect=postgresql.dialect()))
    with pytest.raises(InvalidIndexOptionError, match="vector_router"):
        IndexOptions(vector_router="invalid")
