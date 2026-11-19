from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Text, create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from conftest import MockItem, explain_plan_json, walk_plan_nodes
from paradedb.sqlalchemy import search


pytestmark = pytest.mark.integration


class SynonymBase(DeclarativeBase):
    pass


class ShopSynonym(SynonymBase):
    __tablename__ = "shop_synonyms"

    # The table has no primary key, but the mapper needs one.
    rule: Mapped[str] = mapped_column(Text, primary_key=True)


class Synonym(SynonymBase):
    __tablename__ = "synonyms"

    set_name: Mapped[str] = mapped_column(Text, primary_key=True)
    rule: Mapped[str] = mapped_column(Text, primary_key=True)


@pytest.fixture(scope="module")
def synonym_engine(engine: Engine, db_url: str, paradedb_ready: None) -> Iterator[Engine]:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS shop_synonyms, synonyms"))
        conn.execute(text("CREATE TABLE shop_synonyms (rule pdb.synonym_rule NOT NULL)"))
        conn.execute(text("INSERT INTO shop_synonyms VALUES ('sofa, couch, divan'), ('sneakers => running shoes')"))
        conn.execute(text("CREATE INDEX shop_synonyms_idx ON shop_synonyms USING paradedb (rule)"))
        conn.execute(text("CREATE TABLE synonyms (set_name text NOT NULL, rule pdb.synonym_rule NOT NULL)"))
        conn.execute(
            text("INSERT INTO synonyms VALUES ('shop', 'sneakers => running shoes'), ('outlet', 'sneakers, shoes')")
        )
        for set_name in ("shop", "outlet"):
            conn.execute(
                text(
                    f"CREATE INDEX synonyms_{set_name}_idx ON synonyms USING paradedb (rule) "
                    f"WHERE set_name = '{set_name}'"
                )
            )
    # Prepare every statement. A bind parameter in the subquery's filter stops only the
    # generic plan from using a partial synonym index.
    prepared_engine = create_engine(db_url, connect_args={"prepare_threshold": 0})
    yield prepared_engine
    prepared_engine.dispose()
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE shop_synonyms, synonyms"))


@pytest.fixture()
def synonym_session(synonym_engine: Engine) -> Iterator[Session]:
    with Session(synonym_engine) as session:
        session.execute(text("SET LOCAL plan_cache_mode = force_generic_plan"))
        # Fails a query whose subquery reads its rules from the table and not from a synonym index.
        session.execute(text("SET LOCAL paradedb.planner_warnings = 'error'"))
        yield session


def _synonym_indexes(session: Session, stmt) -> list[str]:
    plan = explain_plan_json(session, stmt)
    return [node["Synonyms"] for node in walk_plan_nodes(plan["Plan"]) if "Synonyms" in node]


def test_expand_with_synonym_index(synonym_session):
    match_any = select(MockItem.id).where(
        search.match_any(MockItem.description, search.expand("couch", ShopSynonym.rule))
    )
    match_all = select(MockItem.id).where(
        search.match_all(MockItem.description, search.expand("leather couch", ShopSynonym.rule))
    )
    for stmt in (match_any, match_all):
        assert _synonym_indexes(synonym_session, stmt) == ["shop_synonyms_idx"]
        assert list(synonym_session.scalars(stmt)) == [24]


@pytest.mark.parametrize(("set_name", "expected_ids"), [("shop", [3]), ("outlet", [3, 4, 5])])
def test_expand_with_partial_synonym_index(synonym_session, set_name, expected_ids):
    stmt = (
        select(MockItem.id)
        .where(
            search.match_any(
                MockItem.description, search.expand("sneakers", Synonym.rule, where=Synonym.set_name == set_name)
            )
        )
        .order_by(MockItem.id)
    )
    assert _synonym_indexes(synonym_session, stmt) == [f"synonyms_{set_name}_idx"]
    assert list(synonym_session.scalars(stmt)) == expected_ids


def test_expand_with_phrase_and_parse(synonym_session):
    phrase = select(MockItem.id).where(
        search.phrase(MockItem.description, search.expand("leather couch", ShopSynonym.rule))
    )
    parse = select(MockItem.id).where(
        search.parse(MockItem.description, search.expand("leather AND couch", ShopSynonym.rule))
    )
    for stmt in (phrase, parse):
        assert _synonym_indexes(synonym_session, stmt) == ["shop_synonyms_idx"]
        assert list(synonym_session.scalars(stmt)) == [24]


def test_expand_with_fuzzy(synonym_session):
    stmt = (
        select(MockItem.id)
        .where(search.match_any(MockItem.description, search.fuzzy(search.expand("couch shoez", ShopSynonym.rule), 1)))
        .order_by(MockItem.id)
    )
    assert _synonym_indexes(synonym_session, stmt) == ["shop_synonyms_idx"]
    assert list(synonym_session.scalars(stmt)) == [3, 4, 5, 24]
