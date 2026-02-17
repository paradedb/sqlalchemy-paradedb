from __future__ import annotations

import os

from sqlalchemy import Integer, String, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy import create_engine

from paradedb.sqlalchemy import search


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)


def main() -> None:
    dsn = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/postgres")
    engine = create_engine(dsn)

    stmt = (
        select(Product.id, Product.description)
        .where(search.phrase_prefix(Product.description, ["run", "sh"]))
        .order_by(Product.id)
        .limit(10)
    )

    with Session(engine) as session:
        for row in session.execute(stmt):
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
