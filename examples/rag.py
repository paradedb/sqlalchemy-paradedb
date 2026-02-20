from __future__ import annotations

from sqlalchemy import Integer, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from common import engine_from_env, setup_documents
from paradedb.sqlalchemy import pdb, search, select_with


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)


def retrieve(query: str, limit: int = 5) -> None:
    engine = engine_from_env()
    setup_documents(engine)

    base = (
        select(Document.id, Document.content)
        .where(search.match_any(Document.content, *query.split()))
        .order_by(pdb.score(Document.id).desc())
        .limit(limit)
    )
    stmt = select_with.score(base, Document.id, label="score")

    with Session(engine) as session:
        for row in session.execute(stmt):
            print(dict(row._mapping))


if __name__ == "__main__":
    retrieve("postgres full text search")
