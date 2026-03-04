"""Comprehensive integration tests for all ParadeDB search operators.

Mirrors the coverage of django-paradedb's test_paradedb_queries.py,
using the mock_items dataset from paradedb.create_bm25_test_table.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from conftest import MockItem, assert_uses_paradedb_scan
from paradedb.sqlalchemy import pdb, search
from paradedb.sqlalchemy.errors import InvalidArgumentError, InvalidMoreLikeThisOptionsError

pytestmark = pytest.mark.integration

RUNNING_IDS = {3}
SHOES_IDS = {3, 4, 5}
RUNNING_OR_WIRELESS_IDS = {3, 12}


def _ids(session, stmt) -> set[int]:
    return set(session.scalars(stmt))


# ---------------------------------------------------------------------------
# Basic operators
# ---------------------------------------------------------------------------


def test_match_any_or_semantics(mock_session):
    """match_any on multiple terms performs OR search."""
    stmt = select(MockItem.id).where(search.match_any(MockItem.description, "running", "wireless"))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert ids == RUNNING_OR_WIRELESS_IDS


def test_match_all_and_semantics(mock_session):
    """match_all requires all terms to be present (AND search)."""
    stmt_all = select(MockItem.id).where(search.match_all(MockItem.description, "running", "shoes"))
    stmt_any = select(MockItem.id).where(search.match_any(MockItem.description, "running", "shoes"))
    assert_uses_paradedb_scan(mock_session, stmt_all, index_name="mock_items_bm25_idx")
    ids_all = _ids(mock_session, stmt_all)
    ids_any = _ids(mock_session, stmt_any)
    assert ids_all == RUNNING_IDS
    assert ids_any == SHOES_IDS


def test_term_exact_token(mock_session):
    """term() does exact token match (=== operator)."""
    term_stmt = select(MockItem.id).where(search.term(MockItem.description, "shoes"))
    assert_uses_paradedb_scan(mock_session, term_stmt, index_name="mock_items_bm25_idx")
    term_ids = _ids(mock_session, term_stmt)
    assert term_ids == SHOES_IDS


def test_phrase_match(mock_session):
    """phrase() matches exact phrase sequence."""
    phrase_stmt = select(MockItem.id).where(search.phrase(MockItem.description, "running shoes"))
    assert_uses_paradedb_scan(mock_session, phrase_stmt, index_name="mock_items_bm25_idx")
    phrase_ids = _ids(mock_session, phrase_stmt)
    assert phrase_ids == RUNNING_IDS


def test_phrase_with_slop(mock_session):
    """phrase() with slop allows intervening tokens."""
    stmt_exact = select(MockItem.id).where(search.phrase(MockItem.description, "running shoes"))
    stmt_slop = select(MockItem.id).where(search.phrase(MockItem.description, "running shoes", slop=2))
    assert_uses_paradedb_scan(mock_session, stmt_slop, index_name="mock_items_bm25_idx")
    ids_slop = _ids(mock_session, stmt_slop)
    ids_exact = _ids(mock_session, stmt_exact)
    # Slop >= 0 should match at least everything exact does
    assert ids_exact.issubset(ids_slop)


def test_regex_match(mock_session):
    """regex() matches patterns against indexed tokens."""
    regex_stmt = select(MockItem.id).where(search.regex(MockItem.description, "run.*"))
    assert_uses_paradedb_scan(mock_session, regex_stmt, index_name="mock_items_bm25_idx")
    regex_ids = _ids(mock_session, regex_stmt)
    assert regex_ids == RUNNING_IDS


# ---------------------------------------------------------------------------
# Fuzzy search
# ---------------------------------------------------------------------------


def test_fuzzy_distance_1(mock_session):
    """match_any(..., distance=1) catches single-character typos."""
    stmt = select(MockItem.id).where(search.match_any(MockItem.description, "runnning", distance=1))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    # Should match "running" with one edit
    assert len(ids) > 0


def test_fuzzy_distance_2(mock_session):
    """match_any(..., distance=2) catches two-character typos."""
    stmt_d1 = select(MockItem.id).where(search.match_any(MockItem.description, "runnning", distance=1))
    stmt_d2 = select(MockItem.id).where(search.match_any(MockItem.description, "runnning", distance=2))
    assert_uses_paradedb_scan(mock_session, stmt_d2, index_name="mock_items_bm25_idx")
    ids_d1 = _ids(mock_session, stmt_d1)
    ids_d2 = _ids(mock_session, stmt_d2)
    # More distance = more or equal matches
    assert ids_d1.issubset(ids_d2)


def test_fuzzy_with_prefix(mock_session):
    """match_any(..., prefix=True) matches prefix expansions."""
    stmt = select(MockItem.id).where(search.match_any(MockItem.description, "runn", distance=1, prefix=True))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert len(ids) > 0


def test_fuzzy_with_transpose_cost_one(mock_session):
    """match_any(..., transpose_cost_one=True) treats transpositions as single edits."""
    stmt = select(MockItem.id).where(
        search.match_any(MockItem.description, "rnnuing", distance=2, transpose_cost_one=True)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert isinstance(ids, set)


def test_fuzzy_with_boost(mock_session):
    """match_any(..., boost=) does not change the result set, only scores."""
    stmt_base = select(MockItem.id).where(search.match_any(MockItem.description, "runnning", distance=1))
    stmt_boost = select(MockItem.id).where(search.match_any(MockItem.description, "runnning", distance=1, boost=2.0))
    assert_uses_paradedb_scan(mock_session, stmt_boost, index_name="mock_items_bm25_idx")
    ids_base = _ids(mock_session, stmt_base)
    ids_boost = _ids(mock_session, stmt_boost)
    assert ids_base == ids_boost


# ---------------------------------------------------------------------------
# Parse / query-string search
# ---------------------------------------------------------------------------


def test_parse_basic(mock_session):
    """parse() executes a query-string search."""
    parse_stmt = select(MockItem.id).where(
        search.parse(MockItem.id, "description:running AND description:shoes", lenient=True)
    )
    assert_uses_paradedb_scan(mock_session, parse_stmt, index_name="mock_items_bm25_idx")
    parse_ids = _ids(mock_session, parse_stmt)
    assert parse_ids == RUNNING_IDS


def test_parse_conjunction_mode_narrows_results(mock_session):
    """parse() with conjunction_mode=True narrows results vs default OR."""
    stmt_default = select(MockItem.id).where(search.parse(MockItem.id, "description:running shoes", lenient=True))
    stmt_conj = select(MockItem.id).where(
        search.parse(MockItem.id, "description:running shoes", lenient=True, conjunction_mode=True)
    )
    assert_uses_paradedb_scan(mock_session, stmt_conj, index_name="mock_items_bm25_idx")
    ids_default = _ids(mock_session, stmt_default)
    ids_conj = _ids(mock_session, stmt_conj)
    assert ids_conj.issubset(ids_default)


# ---------------------------------------------------------------------------
# Phrase prefix / autocomplete
# ---------------------------------------------------------------------------


def test_phrase_prefix_basic(mock_session):
    """phrase_prefix() matches partial last-word for autocomplete."""
    prefix_stmt = select(MockItem.id).where(search.phrase_prefix(MockItem.description, ["running", "sh"]))
    assert_uses_paradedb_scan(mock_session, prefix_stmt, index_name="mock_items_bm25_idx")
    prefix_ids = _ids(mock_session, prefix_stmt)
    assert prefix_ids == RUNNING_IDS


def test_phrase_prefix_max_expansions(mock_session):
    """phrase_prefix() with larger max_expansions finds at least as many results."""
    stmt_50 = select(MockItem.id).where(
        search.phrase_prefix(MockItem.description, ["running", "sh"], max_expansions=50)
    )
    stmt_200 = select(MockItem.id).where(
        search.phrase_prefix(MockItem.description, ["running", "sh"], max_expansions=200)
    )
    assert_uses_paradedb_scan(mock_session, stmt_200, index_name="mock_items_bm25_idx")
    ids_50 = _ids(mock_session, stmt_50)
    ids_200 = _ids(mock_session, stmt_200)
    assert ids_50.issubset(ids_200)


# ---------------------------------------------------------------------------
# Regex phrase
# ---------------------------------------------------------------------------


def test_regex_phrase_basic(mock_session):
    """regex_phrase() matches a sequence of regex patterns."""
    stmt = select(MockItem.id).where(search.regex_phrase(MockItem.description, ["run.*", "shoe.*"]))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert len(ids) > 0


def test_regex_phrase_with_slop(mock_session):
    """regex_phrase() with slop allows tokens between regex matches."""
    stmt_base = select(MockItem.id).where(search.regex_phrase(MockItem.description, ["run.*", "shoe.*"]))
    stmt_slop = select(MockItem.id).where(search.regex_phrase(MockItem.description, ["run.*", "shoe.*"], slop=2))
    assert_uses_paradedb_scan(mock_session, stmt_slop, index_name="mock_items_bm25_idx")
    ids_base = _ids(mock_session, stmt_base)
    ids_slop = _ids(mock_session, stmt_slop)
    assert ids_base.issubset(ids_slop)


# ---------------------------------------------------------------------------
# Proximity search (unordered and ordered)
# ---------------------------------------------------------------------------


def test_near_unordered(mock_session):
    """near() with ordered=False (default) uses ## operator."""
    stmt = select(MockItem.id).where(search.near(MockItem.description, "running", "shoes", distance=3))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert len(ids) > 0


def test_near_ordered(mock_session):
    """near() with ordered=True uses ##> operator; finds terms in order."""
    stmt = select(MockItem.id).where(search.near(MockItem.description, "running", "shoes", distance=3, ordered=True))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert len(ids) > 0


def test_near_ordered_is_subset_of_unordered(mock_session):
    """Ordered proximity is always a subset of unordered proximity."""
    stmt_unordered = select(MockItem.id).where(search.near(MockItem.description, "running", "shoes", distance=3))
    stmt_ordered = select(MockItem.id).where(
        search.near(MockItem.description, "running", "shoes", distance=3, ordered=True)
    )
    ids_unordered = _ids(mock_session, stmt_unordered)
    ids_ordered = _ids(mock_session, stmt_ordered)
    assert ids_ordered.issubset(ids_unordered)


def test_proximity_expr_chain_unordered(mock_session):
    """ProximityExpr chaining with near() unordered."""
    prox = search.prox_array("sleek", "running").near("shoes", distance=1)
    stmt = select(MockItem.id).where(search.proximity(MockItem.description, prox))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert isinstance(ids, set)


def test_proximity_expr_chain_ordered(mock_session):
    """ProximityExpr chaining with near() ordered=True."""
    prox = search.prox_array("running").near("shoes", distance=2, ordered=True)
    stmt = select(MockItem.id).where(search.proximity(MockItem.description, prox))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert isinstance(ids, set)


def test_prox_regex_with_ordered(mock_session):
    """prox_regex chained with ordered near."""
    prox = search.prox_array("running").near(search.prox_regex("sho.*", 50), distance=1, ordered=True)
    stmt = select(MockItem.id).where(search.proximity(MockItem.description, prox))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert isinstance(ids, set)


# ---------------------------------------------------------------------------
# Scoring and ordering
# ---------------------------------------------------------------------------


def test_score_ordering_descending(mock_session):
    """Higher-relevance results appear first when ordering by score desc."""
    stmt = (
        select(MockItem.id, pdb.score(MockItem.id).label("score"))
        .where(search.match_all(MockItem.description, "running", "shoes"))
        .order_by(pdb.score(MockItem.id).desc(), MockItem.id.asc())
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    rows = mock_session.execute(stmt).all()
    assert len(rows) > 0
    scores = [row[1] for row in rows]
    assert scores == sorted(scores, reverse=True)


def test_boost_does_not_change_result_set(mock_session):
    """boost= only affects score, not which rows match."""
    stmt_base = select(MockItem.id).where(search.match_any(MockItem.description, "shoes"))
    stmt_boost = select(MockItem.id).where(search.match_any(MockItem.description, "shoes", boost=2.0))
    assert_uses_paradedb_scan(mock_session, stmt_boost, index_name="mock_items_bm25_idx")
    ids_base = _ids(mock_session, stmt_base)
    ids_boost = _ids(mock_session, stmt_boost)
    assert ids_base == ids_boost


# ---------------------------------------------------------------------------
# More Like This
# ---------------------------------------------------------------------------


def test_more_like_this_by_document_id(mock_session):
    """MLT by single document_id returns similar documents."""
    # Find a running-shoes item first
    shoe_items = list(
        mock_session.scalars(
            select(MockItem.id).where(search.match_all(MockItem.description, "running", "shoes")).limit(1)
        )
    )
    assert shoe_items, "Need at least one running-shoes item"
    source_id = shoe_items[0]

    stmt = select(MockItem.id).where(search.more_like_this(MockItem.id, document_id=source_id, fields=["description"]))
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert source_id in ids


def test_more_like_this_by_document_ids(mock_session):
    """MLT with multiple document_ids ORs the results together."""
    shoe_items = list(
        mock_session.scalars(select(MockItem.id).where(search.match_any(MockItem.description, "shoes")).limit(1))
    )
    wireless_items = list(
        mock_session.scalars(select(MockItem.id).where(search.match_any(MockItem.description, "wireless")).limit(1))
    )
    assert shoe_items and wireless_items, "Need both shoe and wireless items"

    id1, id2 = shoe_items[0], wireless_items[0]
    stmt_combined = select(MockItem.id).where(
        search.more_like_this(MockItem.id, document_ids=[id1, id2], fields=["description"])
    )
    stmt_id1 = select(MockItem.id).where(search.more_like_this(MockItem.id, document_id=id1, fields=["description"]))
    stmt_id2 = select(MockItem.id).where(search.more_like_this(MockItem.id, document_id=id2, fields=["description"]))
    assert_uses_paradedb_scan(mock_session, stmt_combined, index_name="mock_items_bm25_idx")
    ids_combined = _ids(mock_session, stmt_combined)
    ids_1 = _ids(mock_session, stmt_id1)
    ids_2 = _ids(mock_session, stmt_id2)
    # Union of individual MLTs should be a subset of combined (OR logic)
    assert ids_1.issubset(ids_combined)
    assert ids_2.issubset(ids_combined)


def test_more_like_this_by_document_payload(mock_session):
    """MLT with document dict finds similar documents."""
    stmt = select(MockItem.id).where(
        search.more_like_this(MockItem.id, document={"description": "wireless noise-canceling headphones"})
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    assert len(ids) > 0


def test_more_like_this_with_stopwords(mock_session):
    """MLT with stopwords changes which terms drive similarity."""
    shoe_items = list(
        mock_session.scalars(
            select(MockItem.id).where(search.match_all(MockItem.description, "running", "shoes")).limit(1)
        )
    )
    assert shoe_items
    source_id = shoe_items[0]

    ids_baseline = _ids(
        mock_session,
        select(MockItem.id).where(search.more_like_this(MockItem.id, document_id=source_id, fields=["description"])),
    )
    ids_with_stopword = _ids(
        mock_session,
        select(MockItem.id).where(
            search.more_like_this(MockItem.id, document_id=source_id, fields=["description"], stopwords=["shoes"])
        ),
    )
    # Source item should always be included
    assert source_id in ids_baseline
    assert source_id in ids_with_stopword
    # Stopwords should not increase result count
    assert len(ids_with_stopword) <= len(ids_baseline)


def test_more_like_this_with_min_word_length(mock_session):
    """MLT with min_word_length filters short terms from driving similarity."""
    shoe_items = list(
        mock_session.scalars(
            select(MockItem.id).where(search.match_all(MockItem.description, "running", "shoes")).limit(1)
        )
    )
    assert shoe_items
    source_id = shoe_items[0]

    ids_baseline = _ids(
        mock_session,
        select(MockItem.id).where(search.more_like_this(MockItem.id, document_id=source_id, fields=["description"])),
    )
    ids_filtered = _ids(
        mock_session,
        select(MockItem.id).where(
            search.more_like_this(MockItem.id, document_id=source_id, fields=["description"], min_word_length=6)
        ),
    )
    assert source_id in ids_baseline
    assert source_id in ids_filtered


def test_more_like_this_document_ids_empty_raises(mock_session):
    """document_ids=[] raises an error."""
    with pytest.raises(InvalidMoreLikeThisOptionsError, match="document_ids must not be empty"):
        search.more_like_this(MockItem.id, document_ids=[])


def test_more_like_this_multiple_sources_raises(mock_session):
    """Providing more than one input source raises an error."""
    with pytest.raises(InvalidMoreLikeThisOptionsError):
        search.more_like_this(MockItem.id, document_id=1, document={"description": "x"})


def test_more_like_this_no_source_raises(mock_session):
    """Providing no input source raises an error."""
    with pytest.raises(InvalidMoreLikeThisOptionsError):
        search.more_like_this(MockItem.id)


def test_more_like_this_fields_with_document_raises(mock_session):
    """fields= cannot be used with document=."""
    with pytest.raises(InvalidMoreLikeThisOptionsError, match="fields can only be used"):
        search.more_like_this(MockItem.id, document={"description": "x"}, fields=["description"])


# ---------------------------------------------------------------------------
# Range term
# ---------------------------------------------------------------------------


def test_range_term_with_range_type(mock_session):
    """range_term() with explicit range_type casts the bounds literal."""
    from sqlalchemy import Column, Integer, MetaData, Table, text
    from sqlalchemy.dialects.postgresql import INT4RANGE

    engine = mock_session.get_bind()
    metadata = MetaData()
    tbl = Table(
        "rt_items_pq",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("weight_range", INT4RANGE, nullable=False),
    )

    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS rt_items_pq_bm25_idx"))
        conn.execute(text("DROP TABLE IF EXISTS rt_items_pq"))
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            text("CREATE INDEX rt_items_pq_bm25_idx ON rt_items_pq USING bm25 (id, weight_range) WITH (key_field='id')")
        )
        conn.execute(
            text(
                "INSERT INTO rt_items_pq (id, weight_range) VALUES "
                "(1, '[1,4]'::int4range), (2, '[3,9]'::int4range), (3, '[10,12]'::int4range)"
            )
        )

    try:
        with mock_session.get_bind().connect() as conn:
            from sqlalchemy.orm import Session as S

            with S(bind=conn) as s:
                stmt = (
                    select(tbl.c.id)
                    .where(
                        search.range_term(tbl.c.weight_range, "(10, 12]", relation="Intersects", range_type="int4range")
                    )
                    .order_by(tbl.c.id)
                )
                ids = list(s.scalars(stmt))
                assert ids == [3]
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP INDEX IF EXISTS rt_items_pq_bm25_idx"))
            conn.execute(text("DROP TABLE IF EXISTS rt_items_pq"))


def test_range_term_invalid_range_type_raises():
    """range_term() with unknown range_type raises InvalidArgumentError."""
    from sqlalchemy import Column, Integer, MetaData, Table
    from sqlalchemy.dialects.postgresql import INT4RANGE

    metadata = MetaData()
    tbl = Table("dummy", metadata, Column("id", Integer), Column("r", INT4RANGE))

    with pytest.raises(InvalidArgumentError, match="range_type must be one of"):
        search.range_term(tbl.c.r, "[1,5]", relation="Intersects", range_type="badtype")


def test_range_term_invalid_relation_raises():
    """range_term() with unknown relation raises InvalidArgumentError."""
    from sqlalchemy import Column, Integer, MetaData, Table
    from sqlalchemy.dialects.postgresql import INT4RANGE

    metadata = MetaData()
    tbl = Table("dummy2", metadata, Column("id", Integer), Column("r", INT4RANGE))

    with pytest.raises(InvalidArgumentError, match="relation must be one of"):
        search.range_term(tbl.c.r, "[1,5]", relation="BadRelation")


# ---------------------------------------------------------------------------
# Combined / chained filters
# ---------------------------------------------------------------------------


def test_search_combined_with_standard_filter(mock_session):
    """ParadeDB predicate can be combined with standard SQL filters."""
    stmt = (
        select(MockItem.id)
        .where(search.match_any(MockItem.description, "shoes"))
        .where(MockItem.rating >= 4)
        .order_by(MockItem.id)
    )
    assert_uses_paradedb_scan(mock_session, stmt, index_name="mock_items_bm25_idx")
    ids = _ids(mock_session, stmt)
    # All returned items should have rating >= 4
    if ids:
        ratings = list(mock_session.scalars(select(MockItem.rating).where(MockItem.id.in_(ids))))
        assert all(r >= 4 for r in ratings)


def test_all_predicate_matches_everything(mock_session):
    """search.all() matches every indexed row."""
    total = mock_session.scalar(
        select(MockItem.id).where(search.all(MockItem.id)).with_only_columns(__import__("sqlalchemy").func.count())
    )
    # mock_items typically has many rows (paradedb creates ~41)
    assert total is not None and total > 0
