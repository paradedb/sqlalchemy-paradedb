from __future__ import annotations

import json
from typing import Any, Literal

from sqlalchemy import Text, func, literal, literal_column
from sqlalchemy.sql.elements import ClauseElement, ColumnElement

from .errors import InvalidArgumentError
from ._functions import PDBFunctionWithNamedArgs
from ._pdb_cast import PDBCast
from .validation import require_non_empty_string, require_non_negative, require_positive


def _inline_string_literal(value: str) -> ClauseElement:
    return literal_column("'" + value.replace("'", "''") + "'", Text())


def score(field: ColumnElement) -> ClauseElement:
    return func.pdb.score(field)


def alias(field: ColumnElement, name: str) -> ClauseElement:
    require_non_empty_string(name, field_name="name")
    return PDBCast(field.self_group(), "alias", (name,))


def snippet(
    field: ColumnElement,
    *,
    start_tag: str | None = None,
    end_tag: str | None = None,
    max_num_chars: int | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> ClauseElement:
    for name, value in (("max_num_chars", max_num_chars), ("limit", limit), ("offset", offset)):
        if value is not None:
            (require_positive if name == "max_num_chars" else require_non_negative)(value, field_name=name)
    options = [
        ("start_tag", start_tag),
        ("end_tag", end_tag),
        ("max_num_chars", max_num_chars),
        ('"limit"', limit),
        ('"offset"', offset),
    ]
    return PDBFunctionWithNamedArgs("snippet", [field], [(name, value) for name, value in options if value is not None])


def snippets(
    field: ColumnElement,
    *,
    start_tag: str | None = None,
    end_tag: str | None = None,
    max_num_chars: int | None = None,
    limit: int | None = None,
    offset: int | None = None,
    sort_by: str | None = None,
) -> ClauseElement:
    if (start_tag is None) != (end_tag is None):
        raise InvalidArgumentError("start_tag and end_tag must be provided together")
    if start_tag is not None:
        require_non_empty_string(start_tag, field_name="start_tag")
    if end_tag is not None:
        require_non_empty_string(end_tag, field_name="end_tag")
    if max_num_chars is not None:
        require_positive(max_num_chars, field_name="max_num_chars")
    if limit is not None:
        require_positive(limit, field_name="limit")
    if offset is not None:
        require_non_negative(offset, field_name="offset")
    if sort_by is not None:
        require_non_empty_string(sort_by, field_name="sort_by")

    named_args: list[tuple[str, Any]] = []
    if start_tag is not None:
        named_args.append(("start_tag", start_tag))
    if end_tag is not None:
        named_args.append(("end_tag", end_tag))
    if max_num_chars is not None:
        named_args.append(("max_num_chars", max_num_chars))
    if limit is not None:
        named_args.append(('"limit"', limit))
    if offset is not None:
        named_args.append(('"offset"', offset))
    if sort_by is not None:
        named_args.append(("sort_by", sort_by))
    return PDBFunctionWithNamedArgs("snippets", [field], named_args)


def snippet_positions(field: ColumnElement, *, limit: int | None = None, offset: int | None = None) -> ClauseElement:
    options = []
    for name, value in (("limit", limit), ("offset", offset)):
        if value is not None:
            require_non_negative(value, field_name=name)
            options.append((f'"{name}"', value))
    return PDBFunctionWithNamedArgs("snippet_positions", [field], options)


def agg(
    spec: dict[str, Any],
    *,
    approximate: bool | None = None,
    visibility: Literal["transaction", "raw", "threshold"] | None = None,
) -> ClauseElement:
    if not isinstance(spec, dict) or not spec:
        raise InvalidArgumentError("spec must be a non-empty dict")
    payload = json.dumps(spec, separators=(",", ":"), sort_keys=True)
    payload_expr = _inline_string_literal(payload)
    if visibility is not None:
        if approximate is not None:
            raise InvalidArgumentError("Specify visibility or approximate, not both")
        return func.pdb.agg(payload_expr, _inline_string_literal(visibility))
    if approximate is None:
        return func.pdb.agg(payload_expr)
    # pdb.agg() takes an optional second positional boolean: true = exact (default),
    # false = approximate (skip heap visibility checks, ~2-4x faster but may include
    # stale rows). approximate=True → pass false; approximate=False → pass true.
    return func.pdb.agg(payload_expr, literal(not approximate))


def aggregate(
    index: str,
    query: str | ClauseElement,
    spec: dict[str, Any],
    *,
    solve_mvcc: bool | None = None,
    memory_limit: int = 500000000,
    bucket_limit: int | None = None,
    visibility: str | None = None,
) -> ClauseElement:
    """Direct index aggregate; execute with connection.scalar(select(...))."""
    from .search import _query_input

    for name, value in (("memory_limit", memory_limit), ("bucket_limit", bucket_limit)):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
            raise InvalidArgumentError(f"{name} must be a positive integer")
    if visibility is not None and visibility not in ("transaction", "raw", "threshold"):
        raise InvalidArgumentError("visibility must be transaction, raw, or threshold")
    if solve_mvcc is not None and visibility is not None:
        raise InvalidArgumentError("Specify solve_mvcc or visibility, not both")
    return func.paradedb.aggregate(
        PDBCast(literal(index), None, raw_cast="regclass"),
        _query_input(query),
        PDBCast(literal(json.dumps(spec)), None, raw_cast="json"),
        solve_mvcc,
        memory_limit,
        bucket_limit,
        visibility,
    )
