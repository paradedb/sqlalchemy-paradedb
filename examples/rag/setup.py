from __future__ import annotations

import os

from sqlalchemy import Index, Integer, Text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from paradedb.sqlalchemy import indexing
from paradedb.sqlalchemy.tokenizer import unicode


DOCUMENT_ROWS = [
    {"id": 1, "content": "ParadeDB is a Postgres extension for full-text search."},
    {"id": 2, "content": "BM25 ranking helps relevance-based retrieval in PostgreSQL."},
    {"id": 3, "content": "RAG pipelines combine retrieval with LLM generation."},
]


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)


Index(
    "documents_bm25_idx",
    indexing.BM25Field(Document.id),
    indexing.BM25Field(Document.content, tokenizer=unicode(lowercase=True)),
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
        session.add_all(Document(**row) for row in DOCUMENT_ROWS)
        session.commit()
