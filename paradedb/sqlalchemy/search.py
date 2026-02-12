from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Text, func, literal
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import ClauseElement, ColumnElement

from ._pdb_cast import PDBCast
from .errors import InvalidArgumentError

_MATCH_ALL = operators.custom_op("&&&", precedence=5, is_comparison=True)
_MATCH_ANY = operators.custom_op("|||", precedence=5, is_comparison=True)
_TERM = operators.custom_op("===", precedence=5, is_comparison=True)
_PHRASE = operators.custom_op("###", precedence=5, is_comparison=True)
_QUERY = operators.custom_op("@@@", precedence=5, is_comparison=True)
_NEAR = operators.custom_op("##", precedence=5)


def _to_term_payload(*terms: str) -> ClauseElement:
    if not terms:
        raise InvalidArgumentError("at least one search term is required")
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


class ProximityExpr:
    def __init__(self, expr: ClauseElement) -> None:
        self.expr = expr

    def near(self, other: str | ClauseElement | ProximityExpr, *, distance: int) -> ProximityExpr:
        return ProximityExpr(_near_chain(self.expr, other, distance=distance))


def _to_proximity_operand(value: str | ClauseElement | ProximityExpr) -> ClauseElement:
    if isinstance(value, ProximityExpr):
        return value.expr
    if isinstance(value, str):
        return literal(value)
    return value


def _to_proximity_clause(value: str | ClauseElement | ProximityExpr) -> ClauseElement:
    if isinstance(value, ProximityExpr):
        return value.expr
    operand = _to_proximity_operand(value)
    return PDBCast(operand, "proximityclause")


def _near_chain(left: str | ClauseElement | ProximityExpr, right: str | ClauseElement | ProximityExpr, *, distance: int) -> ClauseElement:
    left_expr = _to_proximity_clause(left)
    right_expr = _to_proximity_clause(right)
    return left_expr.operate(_NEAR, literal(distance)).operate(_NEAR, right_expr)


def parse(field: ColumnElement, query: str, *, lenient: bool = False, conjunction_mode: bool = False) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.parse(query, lenient, conjunction_mode))


def phrase_prefix(field: ColumnElement, terms: list[str], *, max_expansions: int = 50) -> ColumnElement[bool]:
    if not terms:
        raise InvalidArgumentError("phrase_prefix requires at least one term")
    return field.operate(_QUERY, func.pdb.phrase_prefix(array(terms, type_=Text()), max_expansions))


def regex_phrase(
    field: ColumnElement,
    terms: list[str],
    *,
    slop: int = 0,
    max_expansions: int = 100,
) -> ColumnElement[bool]:
    if not terms:
        raise InvalidArgumentError("regex_phrase requires at least one term")
    return field.operate(_QUERY, func.pdb.regex_phrase(array(terms, type_=Text()), slop, max_expansions))


def near(field: ColumnElement, left: str | ClauseElement, right: str | ClauseElement, *, distance: int) -> ColumnElement[bool]:
    return field.operate(_QUERY, _near_chain(left, right, distance=distance))


def prox_regex(pattern: str, max_expansions: int = 100) -> ProximityExpr:
    return ProximityExpr(func.pdb.prox_regex(pattern, max_expansions))


def prox_array(*clauses: str | ClauseElement | ProximityExpr) -> ProximityExpr:
    if not clauses:
        raise InvalidArgumentError("prox_array requires at least one clause")
    casted_clauses = [_to_proximity_clause(clause) for clause in clauses]
    return ProximityExpr(func.pdb.prox_array(*casted_clauses))


def proximity(field: ColumnElement, prox: ProximityExpr | ClauseElement) -> ColumnElement[bool]:
    prox_expr = prox.expr if isinstance(prox, ProximityExpr) else prox
    return field.operate(_QUERY, prox_expr)


def more_like_this(
    field: ColumnElement,
    *,
    document_id: Any | None = None,
    document: dict[str, Any] | str | None = None,
    fields: list[str] | None = None,
    min_term_frequency: int | None = None,
    max_query_terms: int | None = None,
    min_doc_frequency: int | None = None,
    max_doc_frequency: int | None = None,
    min_word_length: int | None = None,
    max_word_length: int | None = None,
    boost_factor: float | None = None,
    stopwords: list[str] | None = None,
) -> ColumnElement[bool]:
    if (document_id is None) == (document is None):
        raise InvalidArgumentError("exactly one of document_id or document must be provided")
    if document is not None and fields is not None:
        raise InvalidArgumentError("fields can only be used with document_id")

    if min_term_frequency is not None and min_term_frequency < 0:
        raise InvalidArgumentError("min_term_frequency must be >= 0")
    if max_query_terms is not None and max_query_terms <= 0:
        raise InvalidArgumentError("max_query_terms must be > 0")
    if min_doc_frequency is not None and min_doc_frequency < 0:
        raise InvalidArgumentError("min_doc_frequency must be >= 0")
    if max_doc_frequency is not None and max_doc_frequency < 0:
        raise InvalidArgumentError("max_doc_frequency must be >= 0")
    if (
        min_doc_frequency is not None
        and max_doc_frequency is not None
        and min_doc_frequency > max_doc_frequency
    ):
        raise InvalidArgumentError("min_doc_frequency cannot be greater than max_doc_frequency")
    if min_word_length is not None and min_word_length < 0:
        raise InvalidArgumentError("min_word_length must be >= 0")
    if max_word_length is not None and max_word_length < 0:
        raise InvalidArgumentError("max_word_length must be >= 0")
    if (
        min_word_length is not None
        and max_word_length is not None
        and min_word_length > max_word_length
    ):
        raise InvalidArgumentError("min_word_length cannot be greater than max_word_length")
    if boost_factor is not None and boost_factor < 0:
        raise InvalidArgumentError("boost_factor must be >= 0")
    if stopwords is not None and any((not isinstance(word, str)) or (not word.strip()) for word in stopwords):
        raise InvalidArgumentError("stopwords entries must be non-empty strings")

    options_provided = any(
        option is not None
        for option in (
            min_term_frequency,
            max_query_terms,
            min_doc_frequency,
            max_doc_frequency,
            min_word_length,
            max_word_length,
            boost_factor,
            stopwords,
        )
    )

    args: list[Any] = []
    if document_id is not None:
        args.append(document_id)
        if fields is not None or options_provided:
            args.append(array(fields or [], type_=Text()))
    else:
        payload = document if isinstance(document, str) else json.dumps(document, separators=(",", ":"), sort_keys=True)
        args.append(payload)

    if options_provided:
        args.extend(
            [
                1 if min_term_frequency is None else min_term_frequency,
                25 if max_query_terms is None else max_query_terms,
                0 if min_doc_frequency is None else min_doc_frequency,
                1_000_000 if max_doc_frequency is None else max_doc_frequency,
                0 if min_word_length is None else min_word_length,
                1000 if max_word_length is None else max_word_length,
                0.0 if boost_factor is None else boost_factor,
                array(stopwords or [], type_=Text()),
            ]
        )

    return field.operate(_QUERY, func.pdb.more_like_this(*args))
