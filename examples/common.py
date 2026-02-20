from __future__ import annotations

import os

from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


PRODUCT_ROWS = [
    (1, "Sleek running shoes for daily training", "Footwear", 5),
    (2, "Trail running shoes with durable grip", "Footwear", 4),
    (3, "Wireless noise-canceling headphones", "Electronics", 5),
    (4, "Budget walking sneakers", "Footwear", 2),
    (5, "Artistic ceramic vase", "Home", 3),
]

DOCUMENT_ROWS = [
    (1, "ParadeDB is a Postgres extension for full-text search."),
    (2, "BM25 ranking helps relevance-based retrieval in PostgreSQL."),
    (3, "RAG pipelines combine retrieval with LLM generation."),
]


def engine_from_env() -> Engine:
    dsn = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5443/postgres")
    return create_engine(dsn)


def setup_products(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS products_bm25_idx"))
        conn.execute(text("DROP TABLE IF EXISTS products"))
        conn.execute(
            text(
                """
                CREATE TABLE products (
                  id int primary key,
                  description text not null,
                  category text not null,
                  rating int not null
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX products_bm25_idx ON products USING bm25 (id, description, category, rating) WITH (key_field='id')"
            )
        )
        for row in PRODUCT_ROWS:
            conn.execute(
                text(
                    "INSERT INTO products (id, description, category, rating) VALUES (:id, :description, :category, :rating)"
                ),
                {
                    "id": row[0],
                    "description": row[1],
                    "category": row[2],
                    "rating": row[3],
                },
            )


def setup_documents(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS documents_bm25_idx"))
        conn.execute(text("DROP TABLE IF EXISTS documents"))
        conn.execute(
            text(
                """
                CREATE TABLE documents (
                  id int primary key,
                  content text not null
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX documents_bm25_idx ON documents USING bm25 (id, content) WITH (key_field='id')"))
        for row in DOCUMENT_ROWS:
            conn.execute(
                text("INSERT INTO documents (id, content) VALUES (:id, :content)"),
                {"id": row[0], "content": row[1]},
            )
