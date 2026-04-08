from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from sqlalchemy import Boolean, DateTime, Integer, String, Text, create_engine, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)


class MockItem(Base):
    """Maps to mock_items created by paradedb.create_bm25_test_table."""

    __tablename__ = "mock_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    rating: Mapped[int] = mapped_column(Integer, nullable=False)
    in_stock: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[Any] = mapped_column(DateTime, nullable=False)
    metadata_: Mapped[Any] = mapped_column("metadata", JSONB, nullable=True)


PARADEDB_SCAN_PROVIDERS = {"ParadeDB Base Scan", "ParadeDB Aggregate Scan", "ParadeDB Join Scan"}
ALEMBIC_TEMPLATE_DIR = Path(__file__).with_name("alembic")


@pytest.fixture(scope="session")
def db_url() -> str:
    url = (
        os.environ.get("PARADEDB_TEST_DSN")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:postgres@localhost:5443/postgres"
    )
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


@pytest.fixture(scope="session")
def engine(db_url: str) -> Iterator[Engine]:
    engine = create_engine(db_url, future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_search"))
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
    # Use driver-level execution so SQLAlchemy doesn't treat JSON fragments like
    # `...:2...` inside string literals as bind placeholders.
    explain_result = session.connection().exec_driver_sql(f"EXPLAIN (FORMAT JSON) {sql}").scalar_one()
    return _normalize_explain_plan(explain_result)


def walk_plan_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from walk_plan_nodes(child)


def assert_plan_uses_paradedb_scan(plan: dict[str, Any], *, index_name: str | None = None) -> None:
    root = plan["Plan"]
    nodes = list(walk_plan_nodes(root))

    parade_nodes = [
        node
        for node in nodes
        if node.get("Node Type") == "Custom Scan" and node.get("Custom Plan Provider") in PARADEDB_SCAN_PROVIDERS
    ]
    assert parade_nodes, f"Expected ParadeDB Custom Scan in plan, got: {plan}"
    if index_name is not None:
        assert any(node.get("Index") == index_name for node in parade_nodes), (
            f"Expected index {index_name} in plan: {plan}"
        )


def assert_uses_paradedb_scan(session: Session, stmt, *, index_name: str = "products_bm25_idx") -> None:
    plan = explain_plan_json(session, stmt)
    assert_plan_uses_paradedb_scan(plan, index_name=index_name)


@pytest.fixture(scope="session")
def paradedb_ready(engine: Engine) -> None:
    """Ensure ParadeDB mock_items table exists and is indexed."""
    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS mock_items_bm25_idx"))
        conn.execute(text("DROP TABLE IF EXISTS mock_items"))
        conn.execute(text("CALL paradedb.create_bm25_test_table(schema_name => 'public', table_name => 'mock_items')"))
        conn.execute(
            text(
                "CREATE INDEX mock_items_bm25_idx ON mock_items USING bm25 ("
                "id, description, category, rating, in_stock"
                ") WITH (key_field='id')"
            )
        )


@pytest.fixture()
def mock_session(engine: Engine, paradedb_ready: None) -> Iterator[Session]:
    """Session fixture for tests using the mock_items table."""
    with Session(engine) as session:
        yield session


@pytest.fixture()
def alembic_config_factory(tmp_path: Path, db_url: str):
    def factory(metadata) -> Config:
        script_location = tmp_path / "alembic"
        shutil.copytree(ALEMBIC_TEMPLATE_DIR, script_location)
        (script_location / "versions").mkdir()

        config = Config()
        config.set_main_option("script_location", str(script_location))
        config.set_main_option("sqlalchemy.url", db_url)
        config.attributes["target_metadata"] = metadata
        return config

    return factory
