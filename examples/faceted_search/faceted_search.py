from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from paradedb.sqlalchemy import facets, pdb, search
from setup import Product, engine_from_env, setup_database


def main() -> None:
    engine = engine_from_env()
    setup_database(engine)

    stmt = (
        select(
            pdb.agg(facets.value_count(field="id")).label("count"),
            pdb.agg(facets.avg(field="rating")).label("avg_rating"),
            pdb.agg(facets.percentiles(field="rating", percents=[50, 95])).label("rating_percentiles"),
            pdb.agg(
                facets.top_hits(
                    size=2,
                    sort=[{"rating": "desc"}],
                    docvalue_fields=["id", "rating"],
                )
            ).label("top_hits"),
        )
        .select_from(Product)
        .where(search.match_all(Product.description, "running"))
    )

    with Session(engine) as session:
        row = session.execute(stmt).one()
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
