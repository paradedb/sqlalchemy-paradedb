from __future__ import annotations

from sqlalchemy import Integer, String, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from common import engine_from_env, setup_products
from paradedb.sqlalchemy import search


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)


def main() -> None:
    engine = engine_from_env()
    setup_products(engine)

    stmt = (
        select(Product.id, Product.description)
        .where(search.match_any(Product.description, "running", "shoes"))
        .order_by(Product.id)
        .limit(5)
    )

    with Session(engine) as session:
        for row in session.execute(stmt):
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
