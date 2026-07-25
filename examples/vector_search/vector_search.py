"""Top-K vector search over a ParadeDB index.

Requires a ParadeDB build with vector-in-index support. The ORDER BY metric
must match the index opclass metric, the ``@@@`` predicate (here
``search.all``) is mandatory, and a LIMIT is required for Top-K index
pushdown.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from paradedb.sqlalchemy import search, vector
from setup import Product, engine_from_env, setup_database


def main() -> None:
    engine = engine_from_env()
    setup_database(engine)

    query_embedding = [1.0, 0.0, 0.0]

    stmt = (
        select(Product.id, Product.description)
        .where(search.all(Product.id))
        .order_by(vector.l2_distance(Product.embedding, query_embedding))
        .limit(2)
    )

    with Session(engine) as session:
        for row in session.execute(stmt):
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
