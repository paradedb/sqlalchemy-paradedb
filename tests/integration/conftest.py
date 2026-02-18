from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import Integer, String, Text, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy import create_engine


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)


@pytest.fixture(scope="session")
def db_url() -> str:
    url = os.environ.get("PARADEDB_TEST_DSN") or os.environ.get("DATABASE_URL") or "postgresql://postgres:postgres@localhost:5443/postgres"
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


@pytest.fixture(scope="session")
def engine(db_url: str) -> Iterator[Engine]:
    engine = create_engine(db_url, future=True)
    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS products_bm25_idx"))
    Base.metadata.drop_all(engine, checkfirst=True)
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE INDEX products_bm25_idx ON products USING bm25 (id, description, category, rating) WITH (key_field='id')"
            )
        )
    yield engine
    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS products_bm25_idx"))
    Base.metadata.drop_all(engine, checkfirst=True)
    engine.dispose()


@pytest.fixture()
def session(engine: Engine) -> Iterator[Session]:
    rows = [
        Product(id=1, description="Sleek running shoes for daily training", category="Footwear", rating=5),
        Product(id=2, description="Trail running shoes with durable grip", category="Footwear", rating=4),
        Product(id=3, description="Wireless noise-canceling headphones", category="Electronics", rating=5),
        Product(id=4, description="Budget walking sneakers", category="Footwear", rating=2),
        Product(id=5, description="Artistic ceramic vase", category="Home", rating=3),
    ]

    with Session(engine) as session:
        session.execute(text("TRUNCATE TABLE products RESTART IDENTITY"))
        session.add_all(rows)
        session.commit()
        yield session


def _normalize_explain_plan(result: Any) -> dict[str, Any]:
    if isinstance(result, list):
        if not result:
            raise AssertionError("EXPLAIN returned an empty JSON list")
        return result[0]
    if isinstance(result, dict):
        return result
    raise AssertionError(f"Unexpected EXPLAIN result shape: {type(result)!r}")


def explain_plan_json(session: Session, stmt) -> dict[str, Any]:
    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    explain_result = session.execute(text(f"EXPLAIN (FORMAT JSON) {sql}")).scalar_one()
    return _normalize_explain_plan(explain_result)


def walk_plan_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from walk_plan_nodes(child)


def assert_uses_paradedb_scan(session: Session, stmt, *, index_name: str = "products_bm25_idx") -> None:
    plan = explain_plan_json(session, stmt)
    root = plan["Plan"]
    nodes = list(walk_plan_nodes(root))

    parade_nodes = [
        node
        for node in nodes
        if node.get("Node Type") == "Custom Scan"
        and node.get("Custom Plan Provider") in {"ParadeDB Scan", "ParadeDB Aggregate Scan"}
    ]
    assert parade_nodes, f"Expected ParadeDB Custom Scan in plan, got: {plan}"
    assert any(node.get("Index") == index_name for node in parade_nodes), f"Expected index {index_name} in plan: {plan}"
