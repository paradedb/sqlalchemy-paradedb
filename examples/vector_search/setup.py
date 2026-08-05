from __future__ import annotations

import os

from sqlalchemy import Index, Integer, Text, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from paradedb.sqlalchemy import indexing
from paradedb.sqlalchemy.vector import Vector


PRODUCT_ROWS = [
    {"id": 1, "description": "Sleek running shoes for daily training", "embedding": [1, 0, 0]},
    {"id": 2, "description": "Trail running shoes with durable grip", "embedding": [0.9, 0.1, 0]},
    {"id": 3, "description": "Wireless noise-canceling headphones", "embedding": [0, 1, 0]},
    {"id": 4, "description": "Artistic ceramic vase", "embedding": [0, 0, 1]},
]


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float]] = mapped_column(Vector(3), nullable=False)


Index(
    "search_idx",
    indexing.ParadeDBField(Product.id),
    indexing.ParadeDBField(Product.description),
    indexing.VectorField(Product.embedding, metric="l2"),
    postgresql_using="paradedb",
    postgresql_with={"key_field": "id"},
)


def engine_from_env() -> Engine:
    dsn = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/postgres")
    return create_engine(dsn)


def setup_database(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(Product(**row) for row in PRODUCT_ROWS)
        session.commit()
