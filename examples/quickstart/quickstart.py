from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from paradedb.sqlalchemy import search
from setup import Product, engine_from_env, setup_database


def main() -> None:
    engine = engine_from_env()
    setup_database(engine)

    stmt = (
        select(Product.id, Product.description)
        .where(search.match_any(Product.description, "runnning", distance=1))
        .order_by(Product.id)
        .limit(5)
    )

    with Session(engine) as session:
        for row in session.execute(stmt):
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
