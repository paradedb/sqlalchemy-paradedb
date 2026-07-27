from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from paradedb.sqlalchemy import pdb, search, select_with
from setup import Product, engine_from_env, setup_database


def retrieve(query: str, limit: int = 5) -> None:
    engine = engine_from_env()
    setup_database(engine)

    base = (
        select(Product.id, Product.description)
        .where(search.match_any(Product.description, *query.split()))
        .order_by(pdb.score(Product.id).desc())
        .limit(limit)
    )
    stmt = select_with.score(base, Product.id, label="score")

    with Session(engine) as session:
        for row in session.execute(stmt):
            print(dict(row._mapping))


if __name__ == "__main__":
    retrieve("running shoes for training")
