import pytest
from sqlalchemy import Column, Index, Integer, MetaData, Table, Text, select, text
from sqlalchemy.schema import CreateIndex
from sqlalchemy.exc import DBAPIError

from paradedb import IndexOptions, ParadeDBField, tokenizer
from paradedb.sqlalchemy import pdb, search, select_with
from paradedb.sqlalchemy.alembic import _to_vector_index_options


def test_query_inputs_highlighting_and_resource_limits(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE api_items (id int PRIMARY KEY, description text)"))
        conn.execute(text("INSERT INTO api_items VALUES (1, 'red shoes'), (2, 'red boots'), (3, 'blue shoes')"))
        conn.execute(text("CREATE INDEX api_idx ON api_items USING paradedb (id, description)"))
    items = Table("api_items", MetaData(), autoload_with=engine)
    try:
        conjunction = search.boolean_query(should=["description:red", "description:shoes"], minimum_should_match=2)
        query = search.boolean_query(
            must=[search.disjunction_max([conjunction, "description:boots"], tie_breaker=0.5)],
            must_not=["description:blue"],
        )
        with engine.connect() as conn:
            assert conn.scalars(select(items.c.id).where(search.query(items.c.id, conjunction))).all() == [1]
            assert sorted(conn.scalars(select(items.c.id).where(search.query(items.c.id, query)))) == [1, 2]
            result = conn.scalar(
                select(
                    pdb.aggregate(
                        "api_idx",
                        query,
                        {"count": {"value_count": {"field": "id"}}},
                        memory_limit=10000000,
                        bucket_limit=100,
                        visibility="transaction",
                    )
                )
            )
            assert result["count"]["value"] == 2
            stmt = select(items.c.id).where(search.query(items.c.id, "description:shoes"))
            stmt = select_with.snippet(stmt, items.c.description, max_num_chars=20, limit=1, offset=0)
            stmt = select_with.snippet_positions(stmt, items.c.description, limit=0, offset=1)
            rows = conn.execute(stmt).all()
            assert len(rows) == 2
            assert all(row.snippet is not None for row in rows)
            expected = conn.execute(
                text(
                    'SELECT id, pdb.snippet_positions(description, "limit" => 0, "offset" => 1) FROM api_items WHERE description ||| \'shoes\' ORDER BY id'
                )
            ).all()
            assert sorted((row.id, row.snippet_positions) for row in rows) == expected
            assert (
                conn.scalars(select(items.c.id).where(search.query(items.c.id, 'description:"O\'Reilly"'))).all() == []
            )
            # Named options retain defaults when only one tag is supplied.
            assert (
                conn.scalar(
                    select(pdb.snippet(items.c.description, end_tag="</mark>")).where(
                        search.query(items.c.id, "description:red")
                    )
                )
                is not None
            )
        with pytest.raises(DBAPIError, match="bucket limit was exceeded"), engine.connect() as conn:
            conn.scalar(
                select(
                    pdb.aggregate(
                        "api_idx",
                        "description:red",
                        {"ids": {"terms": {"field": "id", "size": 10}}},
                        memory_limit=10000000,
                        bucket_limit=1,
                    )
                )
            )
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE api_items"))


def test_index_options_and_alembic_round_trip(engine):
    metadata = MetaData()
    items = Table("api_options_items", metadata, Column("id", Integer), Column("description", Text))
    opts = IndexOptions(
        search_tokenizer=tokenizer.simple(options={"lowercase": False}),
        layer_sizes="0",
        background_layer_sizes="100MB, 1GB",
        mutable_segment_rows=1000,
    )
    index = Index(
        "api_options_idx",
        ParadeDBField(items.c.id),
        ParadeDBField(items.c.description),
        postgresql_using="paradedb",
        postgresql_with=dict(opts),
    )
    metadata.create_all(engine)
    try:
        with engine.connect() as conn:
            reloptions = conn.scalar(text("SELECT reloptions FROM pg_class WHERE oid = 'api_options_idx'::regclass"))
        actual = dict(option.split("=", 1) for option in reloptions)
        assert dict(_to_vector_index_options(actual)) == dict(opts)
        assert "search_tokenizer = 'simple(lowercase=false)'" in str(CreateIndex(index).compile(dialect=engine.dialect))
    finally:
        metadata.drop_all(engine)


def test_invalid_api_parameters():
    with pytest.raises(ValueError, match="minimum_should_match"):
        search.boolean_query(minimum_should_match=-1)
    with pytest.raises(ValueError, match="tie_breaker"):
        search.disjunction_max(["description:shoes"], tie_breaker=float("nan"))
    with pytest.raises(ValueError, match="offset"):
        pdb.snippet_positions(Column("description", Text), offset=-1)
    with pytest.raises(ValueError, match="mutable_segment_rows"):
        IndexOptions(mutable_segment_rows=10001)
    with pytest.raises(ValueError, match="not both"):
        pdb.aggregate("idx", "*", {}, solve_mvcc=True, visibility="raw")
