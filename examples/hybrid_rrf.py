from __future__ import annotations

from sqlalchemy import Float, Integer, String, Text, func, literal, select, union_all
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from common import engine_from_env, setup_products
from paradedb.sqlalchemy import pdb, search


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

    query = "running shoes"

    fulltext = (
        select(
            Product.id.label("id"),
            func.row_number().over(order_by=pdb.score(Product.id).desc()).label("rank"),
        )
        .where(search.match_any(Product.description, *query.split()))
        .order_by(pdb.score(Product.id).desc())
        .limit(20)
        .cte("fulltext")
    )

    semantic = (
        select(
            Product.id.label("id"),
            func.row_number().over(order_by=Product.id).label("rank"),
        )
        .limit(20)
        .cte("semantic")
    )

    rrf_k = 60.0
    rrf_fulltext = select(fulltext.c.id, (literal(1.0) / (literal(rrf_k) + fulltext.c.rank)).label("score"))
    rrf_semantic = select(semantic.c.id, (literal(1.0) / (literal(rrf_k) + semantic.c.rank)).label("score"))
    rrf = union_all(rrf_fulltext, rrf_semantic).cte("rrf")

    final_stmt = (
        select(rrf.c.id, func.sum(rrf.c.score).cast(Float).label("hybrid_score"))
        .group_by(rrf.c.id)
        .order_by(func.sum(rrf.c.score).desc())
        .limit(10)
    )

    with Session(engine) as session:
        for row in session.execute(final_stmt):
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
