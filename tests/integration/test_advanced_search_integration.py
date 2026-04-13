from __future__ import annotations

import pytest
from sqlalchemy import select

from conftest import Product, assert_uses_paradedb_scan
from paradedb.sqlalchemy import search
from paradedb.sqlalchemy.errors import InvalidArgumentError


pytestmark = pytest.mark.integration


def test_parse_query_builder_predicate(session):
    stmt = select(Product.id).where(search.parse(Product.id, "description:sleek", lenient=True)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_phrase_prefix_predicate(session):
    stmt = select(Product.id).where(search.phrase_prefix(Product.description, ["running", "sh"])).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_regex_phrase_predicate(session):
    stmt = (
        select(Product.id)
        .where(search.regex_phrase(Product.description, ["run.*", "shoe.*"], slop=1))
        .order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1, 2]


def test_more_like_this_by_document_id(session):
    stmt = (
        select(Product.id)
        .where(search.more_like_this(Product.id, document_id=1, fields=["description"]))
        .order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert 2 in ids


def test_more_like_this_by_document_payload(session):
    stmt = select(Product.id).where(
        search.more_like_this(Product.id, document={"description": "wireless noise-canceling headphones"})
    )
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert 3 in ids


def test_more_like_this_rejects_fields_with_document():
    with pytest.raises(InvalidArgumentError, match="fields can only be used with document_id"):
        search.more_like_this(Product.id, document={"description": "x"}, fields=["description"])


def test_more_like_this_rejects_invalid_numeric_options():
    with pytest.raises(InvalidArgumentError, match="min_term_frequency must be >= 0"):
        search.more_like_this(Product.id, document_id=1, min_term_frequency=-1)

    with pytest.raises(InvalidArgumentError, match="max_query_terms must be > 0"):
        search.more_like_this(Product.id, document_id=1, max_query_terms=0)

    with pytest.raises(InvalidArgumentError, match="min_doc_frequency cannot be greater than max_doc_frequency"):
        search.more_like_this(Product.id, document_id=1, min_doc_frequency=10, max_doc_frequency=2)

    with pytest.raises(InvalidArgumentError, match="min_word_length cannot be greater than max_word_length"):
        search.more_like_this(Product.id, document_id=1, min_word_length=10, max_word_length=2)

    with pytest.raises(InvalidArgumentError, match="boost_factor must be >= 0"):
        search.more_like_this(Product.id, document_id=1, boost_factor=-1.0)


def test_more_like_this_by_document_ids(session):
    """document_ids ORs results from multiple individual MLT queries."""
    stmt_combined = (
        select(Product.id)
        .where(search.more_like_this(Product.id, document_ids=[1, 3], fields=["description"]))
        .order_by(Product.id)
    )
    stmt_id1 = (
        select(Product.id)
        .where(search.more_like_this(Product.id, document_id=1, fields=["description"]))
        .order_by(Product.id)
    )
    stmt_id3 = (
        select(Product.id)
        .where(search.more_like_this(Product.id, document_id=3, fields=["description"]))
        .order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt_combined)
    ids_combined = set(session.scalars(stmt_combined))
    ids_1 = set(session.scalars(stmt_id1))
    ids_3 = set(session.scalars(stmt_id3))
    # Combined should be union of individual results
    assert ids_1.issubset(ids_combined)
    assert ids_3.issubset(ids_combined)


def test_proximity(session):
    prox = search.prox_str("sleek").within(3, "shoes")
    stmt = select(Product.id).where(search.proximity(Product.description, prox)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_proximity_with_boost(session):
    prox = search.prox_str("sleek").within(3, "shoes")
    stmt = select(Product.id).where(search.proximity(Product.description, prox, boost=2)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_proximity_with_const(session):
    prox = search.prox_str("sleek").within(3, "shoes")
    stmt = select(Product.id).where(search.proximity(Product.description, prox, const=2)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_proximity_with_right_associativity(session):
    prox = search.prox_str("sleek").within(1, search.prox_str("running").within(1, "shoes"))
    stmt = select(Product.id).where(search.proximity(Product.description, prox)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert ids == [1]


def test_proximity_with_prox_array_and_regex(session):
    prox = search.prox_array(search.prox_regex("sl.*"), "running").within(1, "shoes").within(3, "running")
    stmt = select(Product.id).where(search.proximity(Product.description, prox)).order_by(Product.id)
    assert_uses_paradedb_scan(session, stmt)
    ids = list(session.scalars(stmt))
    assert 1 in ids


def test_proximity_ordered_predicate(session):
    """near() with ordered=True uses ##> and finds terms in sequence."""
    prox_ordered = search.prox_str("sleek").within(5, "shoes", ordered=True)
    prox_unordered = search.prox_str("sleek").within(5, "shoes")
    stmt_ordered = select(Product.id).where(search.proximity(Product.description, prox_ordered)).order_by(Product.id)
    stmt_unordered = (
        select(Product.id).where(search.proximity(Product.description, prox_unordered)).order_by(Product.id)
    )
    assert_uses_paradedb_scan(session, stmt_ordered)
    ids_ordered = set(session.scalars(stmt_ordered))
    ids_unordered = set(session.scalars(stmt_unordered))
    # Ordered proximity should be a subset of unordered
    assert ids_ordered.issubset(ids_unordered)
    # "Sleek running shoes" — sleek appears before shoes, so ordered should find it
    assert 1 in ids_ordered
