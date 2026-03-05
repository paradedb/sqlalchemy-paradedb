from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from paradedb.sqlalchemy import pdb, search, select_with
from setup import Document, engine_from_env, setup_database


def retrieve(query: str, limit: int = 5) -> None:
    engine = engine_from_env()
    setup_database(engine)

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
