from __future__ import annotations
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from paradedb.sqlalchemy import pdb, search, select_with
from conftest import Product, assert_uses_paradedb_scan
from paradedb.sqlalchemy.errors import SnippetWithFuzzyPredicateError


pytestmark = pytest.mark.integration
RUNNING_PRODUCT_IDS = [1, 2]
WIRELESS_PRODUCT_IDS = [3]


def _sql(stmt) -> str:
    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    return "\n".join(line.rstrip() for line in sql.split("\n")).strip()


def test_match_all_returns_expected_rows(session):
    stmt = select(Product.id).where(search.match_all(Product.description, "running", "shoes")).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_phrase_match_returns_expected_row(session):
    stmt = select(Product.id).where(search.phrase(Product.description, "running shoes")).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_term_exact_token_match(session):
    stmt = select(Product.id).where(search.term(Product.description, "wireless"))
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [3]


def test_regex_match(session):
    stmt = select(Product.id).where(search.regex(Product.description, "run.*")).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_fuzzy_match(session):
    stmt = select(Product.id).where(search.match_any(Product.description, "wirless", distance=1))
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == WIRELESS_PRODUCT_IDS


def test_score_and_ordering(session):
    stmt = (
        select(Product.id, pdb.score(Product.id).label("score"))
        .where(search.match_all(Product.description, "running", "shoes"))
        .order_by(pdb.score(Product.id).desc(), Product.id.asc())
    )
    assert_uses_paradedb_scan(session, stmt)
    rows = session.execute(stmt).all()

    assert [row[0] for row in rows] == [1, 2]
    assert rows[0][1] >= rows[1][1]


def test_snippet_projection(session):
    stmt = (
        select(Product.id, pdb.snippet(Product.description, start_tag="<mark>", end_tag="</mark>").label("snippet"))
        .where(search.match_any(Product.description, "running"))
        .order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt)
    rows = session.execute(stmt).all()

    assert [row[0] for row in rows] == [1, 2]
    assert "<mark>" in rows[0][1]


def test_select_with_helpers(session):
    base = select(Product.id, Product.description).where(search.match_any(Product.description, "running"))
    stmt = select_with.score(base, Product.id, label="search_score")
    stmt = select_with.snippet_positions(stmt, Product.description, label="positions")
    assert_uses_paradedb_scan(session, stmt)

    rows = session.execute(stmt.order_by(Product.id)).all()
    assert [row[0] for row in rows] == RUNNING_PRODUCT_IDS
    assert [row[3] for row in rows] == [[[6, 13]], [[6, 13]]]


def test_select_with_snippet_rejects_fuzzy_predicate():
    base = select(Product.id, Product.description).where(search.match_any(Product.description, "wirless", distance=1))
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippet(base, Product.description)


def test_select_with_snippets_rejects_fuzzy_predicate():
    base = select(Product.id, Product.description).where(search.match_any(Product.description, "wirless", distance=1))
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippets(base, Product.description)


def test_select_with_snippet_positions_rejects_fuzzy_predicate():
    base = select(Product.id, Product.description).where(search.match_any(Product.description, "wirless", distance=1))
    with pytest.raises(SnippetWithFuzzyPredicateError):
        select_with.snippet_positions(base, Product.description)


def test_snippets_and_positions_projection(session):
    stmt = (
        select(
            Product.id,
            pdb.snippets(Product.description, max_num_chars=20).label("snippets"),
            pdb.snippet_positions(Product.description).label("positions"),
        )
        .where(search.match_any(Product.description, "running"))
        .order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt)

    rows = session.execute(stmt).all()
    assert rows == [
        (1, ["Sleek <b>running</b> shoes"], [[6, 13]]),
        (2, ["Trail <b>running</b> shoes"], [[6, 13]]),
    ]


def test_agg_function_projection(session):
    stmt = select(pdb.agg({"value_count": {"field": "id"}})).where(search.all(Product.id))
    assert_uses_paradedb_scan(session, stmt)

    value = session.execute(stmt).scalar_one()
    assert value == {"value": 5.0}


@pytest.mark.parametrize(
    ["expected", "tokenizer", "tokenizer_params"],
    [
        ("pdb.whitespace", "whitespace", None),
        ("pdb.whitespace('alias=my_column')", "whitespace", ["alias=my_column"]),
        ("pdb.unicode_words", "unicode_words", None),
        ("pdb.literal", "literal", None),
        ("pdb.literal_normalized", "literal_normalized", None),
        ("pdb.ngram(3, 3)", "ngram", [3, 3]),
        ("pdb.ngram(3, 3, 'positions=true')", "ngram", [3, 3, "positions=true"]),
        ("pdb.edge_ngram(3, 3)", "edge_ngram", [3, 3]),
        ("pdb.simple", "simple", None),
        ("pdb.regex_pattern('.*')", "regex_pattern", [".*"]),
        ("pdb.chinese_compatible", "chinese_compatible", None),
        ("pdb.lindera('chinese')", "lindera", ["chinese"]),
        ("pdb.icu", "icu", None),
        ("pdb.jieba", "jieba", None),
        ("pdb.source_code", "source_code", None),
    ],
)
def test_all_tokenizers(session, expected: str, tokenizer: str, tokenizer_params: list[Any]) -> None:
    if tokenizer_params:
        stmt = (
            select(Product.id)
            .where(
                search.match_all(
                    Product.description, "running shoes", tokenizer=tokenizer, tokenizer_params=tokenizer_params
                )
            )
            .order_by(Product.id)
        )
    else:
        stmt = (
            select(Product.id)
            .where(search.match_all(Product.description, "running shoes", tokenizer=tokenizer))
            .order_by(Product.id)
        )
    assert_uses_paradedb_scan(session, stmt)
    _ = list(session.scalars(stmt))
    assert (
        _sql(stmt)
        == f"""SELECT products.id
FROM products
WHERE products.description &&& 'running shoes'::{expected} ORDER BY products.id"""
    )
