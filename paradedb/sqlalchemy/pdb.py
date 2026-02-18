from __future__ import annotations

import json
from typing import Any

from sqlalchemy import cast, func, literal
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.elements import ClauseElement, ColumnElement

from .errors import InvalidArgumentError
from ._functions import PDBFunctionWithNamedArgs


def score(field: ColumnElement) -> ClauseElement:
    return func.pdb.score(field)


def snippet(
    field: ColumnElement,
    *,
    start_tag: str | None = None,
    end_tag: str | None = None,
    max_num_chars: int | None = None,
) -> ClauseElement:
    if (start_tag is None) != (end_tag is None):
        raise InvalidArgumentError("start_tag and end_tag must be provided together")

    args: list[Any] = [field]
    if start_tag is not None and end_tag is not None:
        args.extend([start_tag, end_tag])
    if max_num_chars is not None:
        args.append(max_num_chars)
    return func.pdb.snippet(*args)


def snippets(
    field: ColumnElement,
    *,
    max_num_chars: int | None = None,
    limit: int | None = None,
    offset: int | None = None,
    sort_by: str | None = None,
) -> ClauseElement:
    named_args: list[tuple[str, Any]] = []
    if max_num_chars is not None:
        named_args.append(("max_num_chars", max_num_chars))
    if limit is not None:
        named_args.append(("\"limit\"", limit))
    if offset is not None:
        named_args.append(("\"offset\"", offset))
    if sort_by is not None:
        named_args.append(("sort_by", sort_by))
    return PDBFunctionWithNamedArgs("snippets", [field], named_args)


def snippet_positions(field: ColumnElement) -> ClauseElement:
    return func.pdb.snippet_positions(field)


def agg(spec: dict[str, Any], *, approximate: bool | None = None) -> ClauseElement:
    payload = json.dumps(spec, separators=(",", ":"), sort_keys=True)
    payload_expr = cast(literal(payload), JSONB)
    if approximate is None:
        return func.pdb.agg(payload_expr)
    return func.pdb.agg(payload_expr, approximate=approximate)
