from __future__ import annotations

import os

from sqlalchemy import Index, Integer, String, Text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from paradedb.sqlalchemy import indexing


PRODUCT_ROWS = [
    {"id": 1, "description": "Sleek running shoes for daily training", "category": "Footwear", "rating": 5},
    {"id": 2, "description": "Trail running shoes with durable grip", "category": "Footwear", "rating": 4},
    {"id": 3, "description": "Wireless noise-canceling headphones", "category": "Electronics", "rating": 5},
    {"id": 4, "description": "Budget walking sneakers", "category": "Footwear", "rating": 2},
    {"id": 5, "description": "Artistic ceramic vase", "category": "Home", "rating": 3},
]


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)


Index(
    "products_bm25_idx",
    indexing.BM25Field(Product.id),
    indexing.BM25Field(Product.description, tokenizer=indexing.tokenize.unicode(lowercase=True)),
    indexing.BM25Field(Product.category, tokenizer=indexing.tokenize.literal()),
    indexing.BM25Field(Product.rating),
    postgresql_using="bm25",
    postgresql_with={"key_field": "id"},
)


def engine_from_env() -> Engine:
    dsn = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5443/postgres")
    return create_engine(dsn)


def setup_database(engine: Engine) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(Product(**row) for row in PRODUCT_ROWS)
        session.commit()
