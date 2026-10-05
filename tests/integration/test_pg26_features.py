from unittest.mock import MagicMock
from alembic.operations.ops import UpgradeOps
from paradedb.sqlalchemy.alembic import _compare_paradedb_indexes
import json

from sqlalchemy import Column, Index, Integer, MetaData, Table, Text, select, text
from paradedb import (
    IndexOptions,
    ParadeDBField,
    tokenizer,
    Vector,
    VectorField,
    agg,
    paradedb_vector_config,
    paradedb_vector_info,
    paradedb_vector_estimator_info,
)
from paradedb.sqlalchemy.alembic import _to_vector_index_options


def test_partitioning_quantization_and_diagnostics(engine):
    metadata = MetaData()
    items = Table(
        "pg26_items",
        metadata,
        Column("id", Integer),
        Column("rating", Integer),
        Column("description", Text),
        Column("embedding", Vector(64)),
    )
    opts = IndexOptions(
        partition_by="rating,id",
        target_segment_count=8,
        vector_fields={"embedding": {"quantization": False}},
    )
    Index(
        "pg26_idx",
        ParadeDBField(items.c.id),
        ParadeDBField(items.c.rating),
        ParadeDBField(items.c.description, tokenizer=tokenizer.simple(options={"pnorms": True})),
        ParadeDBField(
            items.c.description, tokenizer=tokenizer.jieba(options={"alias": "description_jieba", "search_mode": False})
        ),
        ParadeDBField(
            items.c.description,
            tokenizer=tokenizer.chinese_compatible(options={"alias": "description_chinese", "chinese_convert": "t2s"}),
        ),
        VectorField(items.c.embedding),
        postgresql_using="paradedb",
        postgresql_with=dict(opts),
    )
    metadata.create_all(engine)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO pg26_items SELECT i, i % 3, 'partitioned shoes', ARRAY(SELECT sin(i*j)::real FROM generate_series(1,64) j)::vector FROM generate_series(1, 2048) i"
                )
            )
            values = conn.execute(text("SELECT reloptions FROM pg_class WHERE oid = 'pg26_idx'::regclass")).scalar_one()
            options = dict(option.split("=", 1) for option in values)
            assert options["partition_by"] == "rating,id"
            assert options["target_segment_count"] == "8"
            assert json.loads(options["vector_fields"])["embedding"]["quantization"] is False
            assert _to_vector_index_options(options) == opts
            context = MagicMock(connection=conn, metadata=metadata)
            changes = UpgradeOps([])
            _compare_paradedb_indexes(context, changes, {None})
            assert not [op for op in changes.ops if getattr(op, "index_name", None) == "pg26_idx"]
            assert (
                conn.execute(
                    text("SELECT COUNT(*) FROM pg26_items WHERE description @@@ 'shoes' AND rating = 1")
                ).scalar_one()
                == 683
            )
            for visibility in ("transaction", "raw", "threshold"):
                result = conn.execute(
                    select(agg({"value_count": {"field": "id"}}, visibility=visibility)).select_from(items)
                ).scalar_one()
                assert result["value"] == 2048
        assert paradedb_vector_config(engine, "pg26_idx", "embedding")[0]["quantized"] is False
        assert paradedb_vector_info(engine, "pg26_idx", "embedding")
        with engine.begin() as conn:
            conn.exec_driver_sql(
                'ALTER INDEX pg26_idx SET (target_segment_count = 1, max_leaf_size = 16, vector_fields = \'{"embedding":{"quantization":true}}\')'
            )
            conn.execute(text("REINDEX INDEX pg26_idx"))
        assert paradedb_vector_config(engine, "pg26_idx", "embedding")[0]["quantized"] is True
        assert isinstance(paradedb_vector_estimator_info(engine, "pg26_idx", "embedding"), list)
        assert isinstance(paradedb_vector_estimator_info(engine, "pg26_idx", "embedding", [[0.1] * 64]), list)
    finally:
        metadata.drop_all(engine)
