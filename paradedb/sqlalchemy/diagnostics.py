from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy.engine import Engine


def _exec_and_collect(conn, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    # exec_driver_sql requires a tuple for single-row execution, or None for no params.
    result = conn.exec_driver_sql(sql, tuple(params) if params else None)
    columns = list(result.keys())
    return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]


def paradedb_indexes(engine: Engine) -> list[dict[str, Any]]:
    """Return metadata for all BM25 indexes from ``pdb.indexes()``."""
    with engine.connect() as conn:
        return _exec_and_collect(conn, "SELECT * FROM pdb.indexes()", [])


def paradedb_index_segments(engine: Engine, index: str) -> list[dict[str, Any]]:
    """Return segment metadata for a BM25 index from ``pdb.index_segments()``."""
    with engine.connect() as conn:
        return _exec_and_collect(
            conn, "SELECT * FROM pdb.index_segments(%s::regclass)", [index]
        )


def paradedb_verify_index(
    engine: Engine,
    index: str,
    *,
    heapallindexed: bool = False,
    sample_rate: float | None = None,
    report_progress: bool = False,
    verbose: bool = False,
    on_error_stop: bool = False,
    segment_ids: Sequence[int] | None = None,
) -> list[dict[str, Any]]:
    """Run ``pdb.verify_index()`` for one BM25 index."""
    sql_parts = ["SELECT * FROM pdb.verify_index(%s::regclass"]
    params: list[Any] = [index]
    if heapallindexed:
        sql_parts.append(", heapallindexed => %s::boolean")
        params.append(heapallindexed)
    if sample_rate is not None:
        sql_parts.append(", sample_rate => %s::double precision")
        params.append(sample_rate)
    if report_progress:
        sql_parts.append(", report_progress => %s::boolean")
        params.append(report_progress)
    if verbose:
        sql_parts.append(", verbose => %s::boolean")
        params.append(verbose)
    if on_error_stop:
        sql_parts.append(", on_error_stop => %s::boolean")
        params.append(on_error_stop)
    if segment_ids is not None:
        sql_parts.append(", segment_ids => %s::int[]")
        params.append(list(segment_ids))
    sql_parts.append(")")
    with engine.connect() as conn:
        return _exec_and_collect(conn, "".join(sql_parts), params)


def paradedb_verify_all_indexes(
    engine: Engine,
    *,
    schema_pattern: str | None = None,
    index_pattern: str | None = None,
    heapallindexed: bool = False,
    sample_rate: float | None = None,
    report_progress: bool = False,
    on_error_stop: bool = False,
) -> list[dict[str, Any]]:
    """Run ``pdb.verify_all_indexes()`` across BM25 indexes."""
    named_params: list[tuple[str, str, Any]] = []
    if schema_pattern is not None:
        named_params.append(("schema_pattern", "text", schema_pattern))
    if index_pattern is not None:
        named_params.append(("index_pattern", "text", index_pattern))
    if heapallindexed:
        named_params.append(("heapallindexed", "boolean", heapallindexed))
    if sample_rate is not None:
        named_params.append(("sample_rate", "double precision", sample_rate))
    if report_progress:
        named_params.append(("report_progress", "boolean", report_progress))
    if on_error_stop:
        named_params.append(("on_error_stop", "boolean", on_error_stop))

    sql_parts = ["SELECT * FROM pdb.verify_all_indexes("]
    params: list[Any] = []
    if named_params:
        sql_parts.append(
            ", ".join(
                f"{name} => %s::{pg_type}"
                for name, pg_type, _ in named_params
            )
        )
        params.extend(value for _, _, value in named_params)
    sql_parts.append(")")

    with engine.connect() as conn:
        return _exec_and_collect(conn, "".join(sql_parts), params)


__all__ = [
    "paradedb_index_segments",
    "paradedb_indexes",
    "paradedb_verify_all_indexes",
    "paradedb_verify_index",
]
