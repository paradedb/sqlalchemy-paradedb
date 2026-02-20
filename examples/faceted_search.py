from __future__ import annotations

from sqlalchemy import Integer, String, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from common import engine_from_env, setup_products
from paradedb.sqlalchemy import facets, pdb, search


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)


def main() -> None:
    engine = engine_from_env()
    setup_products(engine)

    stmt = (
        select(
            pdb.agg(facets.value_count(field="id")).label("count"),
            pdb.agg(facets.avg(field="rating")).label("avg_rating"),
        )
        .select_from(Product)
        .where(search.match_all(Product.description, "running"))
    )

    with Session(engine) as session:
        row = session.execute(stmt).one()
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
