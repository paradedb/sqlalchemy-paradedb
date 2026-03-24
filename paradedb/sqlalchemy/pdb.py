from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Text, func, literal, literal_column
from sqlalchemy.sql.elements import ClauseElement, ColumnElement

from .errors import InvalidArgumentError
from ._functions import PDBFunctionWithNamedArgs
from .validation import require_non_empty_string, require_non_negative, require_positive


# TODO: dedupe this?
def _inline_string_literal(value: str) -> ClauseElement:
    return literal_column("'" + value.replace("'", "''") + "'", Text())


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
    if start_tag is not None:
        require_non_empty_string(start_tag, field_name="start_tag")
    if end_tag is not None:
        require_non_empty_string(end_tag, field_name="end_tag")
    if max_num_chars is not None:
        require_positive(max_num_chars, field_name="max_num_chars")

    if max_num_chars is not None and start_tag is None and end_tag is None:
        # ParadeDB versions in CI don't support pdb.snippet(field, max_num_chars)
        # directly. Supplying default tags targets the supported 4-arg form.
        start_tag = "<b>"
        end_tag = "</b>"

    args: list[Any] = [field]
    if start_tag is not None and end_tag is not None:
        args.extend([start_tag, end_tag])
    if max_num_chars is not None:
        args.append(max_num_chars)
    return func.pdb.snippet(*args)


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


def snippet_positions(field: ColumnElement) -> ClauseElement:
    return func.pdb.snippet_positions(field)


def agg(spec: dict[str, Any], *, approximate: bool | None = None) -> ClauseElement:
    if not isinstance(spec, dict) or not spec:
        raise InvalidArgumentError("spec must be a non-empty dict")
    payload = json.dumps(spec, separators=(",", ":"), sort_keys=True)
    payload_expr = _inline_string_literal(payload)
    if approximate is None:
        return func.pdb.agg(payload_expr)
    # pdb.agg() takes an optional second positional boolean: true = exact (default),
    # false = approximate (skip heap visibility checks, ~2-4x faster but may include
    # stale rows). approximate=True → pass false; approximate=False → pass true.
    return func.pdb.agg(payload_expr, literal(not approximate))
