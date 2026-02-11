from __future__ import annotations

from sqlalchemy import Text, func, literal
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import ClauseElement, ColumnElement

from ._pdb_cast import PDBCast

_MATCH_ALL = operators.custom_op("&&&", precedence=5, is_comparison=True)
_MATCH_ANY = operators.custom_op("|||", precedence=5, is_comparison=True)
_TERM = operators.custom_op("===", precedence=5, is_comparison=True)
_PHRASE = operators.custom_op("###", precedence=5, is_comparison=True)
_QUERY = operators.custom_op("@@@", precedence=5, is_comparison=True)


def _to_term_payload(*terms: str) -> ClauseElement:
    if not terms:
        raise ValueError("at least one search term is required")
    if len(terms) == 1:
        return literal(terms[0])
    return array(list(terms), type_=Text())


def _apply_boost(expr: ClauseElement, boost: float | None) -> ClauseElement:
    if boost is None:
        return expr
    return PDBCast(expr, "boost", (boost,))


def match_all(field: ColumnElement, *terms: str, boost: float | None = None) -> ColumnElement[bool]:
    payload = _apply_boost(_to_term_payload(*terms), boost)
    return field.operate(_MATCH_ALL, payload)


def match_any(field: ColumnElement, *terms: str, boost: float | None = None) -> ColumnElement[bool]:
    payload = _apply_boost(_to_term_payload(*terms), boost)
    return field.operate(_MATCH_ANY, payload)


def term(field: ColumnElement, value: str, boost: float | None = None) -> ColumnElement[bool]:
    payload = _apply_boost(literal(value), boost)
    return field.operate(_TERM, payload)


def phrase(field: ColumnElement, value: str, *, slop: int | None = None, boost: float | None = None) -> ColumnElement[bool]:
    payload: ClauseElement = literal(value)
    if slop is not None:
        payload = PDBCast(payload, "slop", (slop,))
    payload = _apply_boost(payload, boost)
    return field.operate(_PHRASE, payload)


def fuzzy(
    field: ColumnElement,
    value: str,
    *,
    distance: int,
    prefix: bool | None = None,
    transpose_cost_one: bool | None = None,
    boost: float | None = None,
) -> ColumnElement[bool]:
    args: list[object] = [distance]
    if prefix is not None or transpose_cost_one is not None:
        args.append(bool(prefix))
    if transpose_cost_one is not None:
        args.append(bool(transpose_cost_one))

    payload = PDBCast(literal(value), "fuzzy", args)
    payload = _apply_boost(payload, boost)
    return field.operate(_TERM, payload)


def regex(field: ColumnElement, pattern: str) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.regex(pattern))


def all(field: ColumnElement) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.all())
