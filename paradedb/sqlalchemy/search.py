from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Text, func, literal, literal_column, or_
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import ClauseElement, ColumnElement

from ._functions import PDBFunctionWithNamedArgs
from ._pdb_cast import PDBCast
from .errors import InvalidArgumentError, InvalidMoreLikeThisOptionsError
from .validation import (
    require_non_empty_sequence,
    require_non_empty_string,
    require_non_empty_strings,
    require_non_negative,
    require_ordered_bounds,
    require_positive,
)

_VALID_RANGE_RELATIONS: frozenset[str] = frozenset({"Intersects", "Contains", "Within", "ContainsOrIntersects"})
_VALID_RANGE_TYPES: frozenset[str] = frozenset(
    {"int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange"}
)

_MATCH_ALL: Any = operators.custom_op("&&&", precedence=5, is_comparison=True)
_MATCH_ANY: Any = operators.custom_op("|||", precedence=5, is_comparison=True)
_TERM: Any = operators.custom_op("===", precedence=5, is_comparison=True)
_PHRASE: Any = operators.custom_op("###", precedence=5, is_comparison=True)
_QUERY: Any = operators.custom_op("@@@", precedence=5, is_comparison=True)
_NEAR: Any = operators.custom_op("##", precedence=5)
_NEAR_ORDERED: Any = operators.custom_op("##>", precedence=5)


def _to_term_payload(*terms: str) -> ClauseElement:
    if not terms:
        raise InvalidArgumentError("at least one search term is required")
    require_non_empty_strings(terms, field_name="terms")
    if len(terms) == 1:
        return literal(terms[0])
    return array(list(terms), type_=Text())


def _apply_boost(expr: ClauseElement, boost: float | None) -> ClauseElement:
    if boost is None:
        return expr
    return PDBCast(expr, "boost", (boost,))


def _apply_fuzzy(
    expr: ClauseElement,
    *,
    distance: int | None = None,
    prefix: bool = False,
    transpose_cost_one: bool = False,
) -> ClauseElement:
    if distance is not None and (distance < 0 or distance > 2):
        raise InvalidArgumentError("distance must be between 0 and 2")

    if distance is None and not prefix and not transpose_cost_one:
        return expr

    args: list[object] = [1 if distance is None else distance]
    if prefix or transpose_cost_one:
        args.append(prefix)
    if transpose_cost_one:
        args.append(True)
    return PDBCast(expr, "fuzzy", args)


def match_all(
    field: ColumnElement,
    *terms: str,
    boost: float | None = None,
    distance: int | None = None,
    prefix: bool = False,
    transpose_cost_one: bool = False,
) -> ColumnElement[bool]:
    payload = _to_term_payload(*terms)
    payload = _apply_fuzzy(payload, distance=distance, prefix=prefix, transpose_cost_one=transpose_cost_one)
    payload = _apply_boost(payload, boost)
    return field.operate(_MATCH_ALL, payload)


def match_any(
    field: ColumnElement,
    *terms: str,
    boost: float | None = None,
    distance: int | None = None,
    prefix: bool = False,
    transpose_cost_one: bool = False,
) -> ColumnElement[bool]:
    payload = _to_term_payload(*terms)
    payload = _apply_fuzzy(payload, distance=distance, prefix=prefix, transpose_cost_one=transpose_cost_one)
    payload = _apply_boost(payload, boost)
    return field.operate(_MATCH_ANY, payload)


def term(
    field: ColumnElement,
    value: str,
    boost: float | None = None,
    *,
    distance: int | None = None,
    prefix: bool = False,
    transpose_cost_one: bool = False,
) -> ColumnElement[bool]:
    require_non_empty_string(value, field_name="value")
    payload: ClauseElement = literal(value)
    payload = _apply_fuzzy(payload, distance=distance, prefix=prefix, transpose_cost_one=transpose_cost_one)
    payload = _apply_boost(payload, boost)
    return field.operate(_TERM, payload)


def phrase(
    field: ColumnElement, value: str, *, slop: int | None = None, boost: float | None = None
) -> ColumnElement[bool]:
    require_non_empty_string(value, field_name="value")
    if slop is not None:
        require_non_negative(slop, field_name="slop")
    payload: ClauseElement = literal(value)
    if slop is not None:
        payload = PDBCast(payload, "slop", (slop,))
    payload = _apply_boost(payload, boost)
    return field.operate(_PHRASE, payload)


def regex(field: ColumnElement, pattern: str) -> ColumnElement[bool]:
    require_non_empty_string(pattern, field_name="pattern")
    return field.operate(_QUERY, func.pdb.regex(pattern))


def all(field: ColumnElement) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.all())


class ProximityExpr:
    def __init__(self, expr: ClauseElement) -> None:
        self.expr = expr

    def near(
        self,
        other: str | ClauseElement | ProximityExpr | None = None,
        *,
        distance: int,
        ordered: bool = False,
        right_pattern: str | None = None,
        max_expansions: int = 100,
    ) -> ProximityExpr:
        right = _resolve_near_operand(other, right_pattern=right_pattern, max_expansions=max_expansions)
        return ProximityExpr(_near_chain(self.expr, right, distance=distance, ordered=ordered))


def _to_proximity_operand(value: str | ClauseElement | ProximityExpr) -> ClauseElement:
    if isinstance(value, ProximityExpr):
        return value.expr
    if isinstance(value, str):
        require_non_empty_string(value, field_name="clause")
        return literal(value)
    return value


def _to_proximity_clause(value: str | ClauseElement | ProximityExpr) -> ClauseElement:
    if isinstance(value, ProximityExpr):
        return value.expr
    operand = _to_proximity_operand(value)
    return PDBCast(operand, "proximityclause")


def _near_chain(
    left: str | ClauseElement | ProximityExpr,
    right: str | ClauseElement | ProximityExpr,
    *,
    distance: int,
    ordered: bool = False,
) -> ClauseElement:
    require_non_negative(distance, field_name="distance")
    left_expr = _to_proximity_clause(left)
    right_expr = _to_proximity_clause(right)
    op = _NEAR_ORDERED if ordered else _NEAR
    return left_expr.operate(op, literal(distance)).operate(op, right_expr)


def _resolve_near_operand(
    right: str | ClauseElement | ProximityExpr | None,
    *,
    right_pattern: str | None,
    max_expansions: int,
) -> str | ClauseElement | ProximityExpr:
    if right_pattern is not None:
        if right is not None:
            raise InvalidArgumentError("right and right_pattern cannot be used together")
        require_non_empty_string(right_pattern, field_name="right_pattern")
        require_non_negative(max_expansions, field_name="max_expansions")
        return prox_regex(right_pattern, max_expansions)
    if right is None:
        raise InvalidArgumentError("right is required unless right_pattern is provided")
    return right


def parse(
    field: ColumnElement, query: str, *, lenient: bool = False, conjunction_mode: bool = False
) -> ColumnElement[bool]:
    require_non_empty_string(query, field_name="query")
    return field.operate(_QUERY, func.pdb.parse(query, lenient, conjunction_mode))


def phrase_prefix(field: ColumnElement, terms: list[str], *, max_expansions: int = 50) -> ColumnElement[bool]:
    if not terms:
        raise InvalidArgumentError("phrase_prefix requires at least one term")
    require_non_empty_strings(terms, field_name="terms")
    require_positive(max_expansions, field_name="max_expansions")
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
    require_non_empty_strings(terms, field_name="terms")
    require_non_negative(slop, field_name="slop")
    require_positive(max_expansions, field_name="max_expansions")
    return field.operate(_QUERY, func.pdb.regex_phrase(array(terms, type_=Text()), slop, max_expansions))


def near(
    field: ColumnElement,
    left: str | ClauseElement | ProximityExpr,
    right: str | ClauseElement | ProximityExpr | None = None,
    *,
    distance: int,
    ordered: bool = False,
    right_pattern: str | None = None,
    max_expansions: int = 100,
) -> ColumnElement[bool]:
    right_operand = _resolve_near_operand(right, right_pattern=right_pattern, max_expansions=max_expansions)
    return field.operate(_QUERY, _near_chain(left, right_operand, distance=distance, ordered=ordered))


def prox_regex(pattern: str, max_expansions: int = 100) -> ProximityExpr:
    require_non_empty_string(pattern, field_name="pattern")
    require_non_negative(max_expansions, field_name="max_expansions")
    return ProximityExpr(func.pdb.prox_regex(pattern, max_expansions))


def prox_array(*clauses: str | ClauseElement | ProximityExpr) -> ProximityExpr:
    if not clauses:
        raise InvalidArgumentError("prox_array requires at least one clause")
    casted_clauses = [_to_proximity_clause(clause) for clause in clauses]
    return ProximityExpr(func.pdb.prox_array(*casted_clauses))


def proximity(field: ColumnElement, prox: ProximityExpr | ClauseElement) -> ColumnElement[bool]:
    prox_expr = prox.expr if isinstance(prox, ProximityExpr) else prox
    return field.operate(_QUERY, prox_expr)


def range_term(
    field: ColumnElement,
    bounds: str,
    *,
    relation: str = "Intersects",
    range_type: str | None = None,
) -> ColumnElement[bool]:
    """Match rows where a range-typed field satisfies a range predicate.

    Args:
        field: A range-typed column (int4range, daterange, tstzrange, etc.).
        bounds: A range literal string, e.g. ``"[3,9]"``, ``"(3,9]"``.
        relation: One of ``"Intersects"``, ``"Contains"``, ``"Within"``,
                  ``"ContainsOrIntersects"``. Defaults to ``"Intersects"``.
        range_type: Optional PostgreSQL range type for explicit casting, e.g.
                    ``"int4range"``, ``"int8range"``, ``"numrange"``,
                    ``"daterange"``, ``"tsrange"``, ``"tstzrange"``.
                    When provided, generates ``'bounds'::range_type`` cast.

    Generates::

        field @@@ pdb.range_term('[3,9]', 'Contains')
        field @@@ pdb.range_term('[3,9]'::int4range, 'Contains')
    """
    require_non_empty_string(bounds, field_name="bounds")
    if relation not in _VALID_RANGE_RELATIONS:
        raise InvalidArgumentError(f"relation must be one of: {', '.join(sorted(_VALID_RANGE_RELATIONS))}")
    escaped_relation = relation.replace("'", "''")
    relation_arg: ClauseElement = literal_column(f"'{escaped_relation}'")
    if range_type is not None:
        if range_type not in _VALID_RANGE_TYPES:
            raise InvalidArgumentError(f"range_type must be one of: {', '.join(sorted(_VALID_RANGE_TYPES))}")
        escaped = bounds.replace("'", "''")
        bounds_arg: ClauseElement = literal_column(f"'{escaped}'::{range_type}")
    else:
        bounds_arg = literal(bounds)
    return field.operate(_QUERY, func.pdb.range_term(bounds_arg, relation_arg))


def more_like_this(
    field: ColumnElement,
    *,
    document_id: Any | None = None,
    document_ids: list[Any] | None = None,
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
    error_cls = InvalidMoreLikeThisOptionsError

    sources_provided = sum(x is not None for x in (document_id, document_ids, document))
    if sources_provided != 1:
        raise error_cls("exactly one of document_id, document_ids, or document must be provided")
    if document_ids is not None and len(document_ids) == 0:
        raise error_cls("document_ids must not be empty")
    if document is not None and fields is not None:
        raise error_cls("fields can only be used with document_id or document_ids")
    if fields is not None:
        require_non_empty_sequence(fields, field_name="fields", error_cls=error_cls)
        require_non_empty_strings(fields, field_name="fields", error_cls=error_cls)
    if document_ids is not None and any(doc_id is None for doc_id in document_ids):
        raise error_cls("document_ids entries cannot be null")
    if isinstance(document, str):
        require_non_empty_string(document, field_name="document", error_cls=error_cls)
    if isinstance(document, dict) and not document:
        raise error_cls("document must not be empty")

    if min_term_frequency is not None:
        require_non_negative(min_term_frequency, field_name="min_term_frequency", error_cls=error_cls)
    if max_query_terms is not None:
        require_positive(max_query_terms, field_name="max_query_terms", error_cls=error_cls)
    if min_doc_frequency is not None:
        require_non_negative(min_doc_frequency, field_name="min_doc_frequency", error_cls=error_cls)
    if max_doc_frequency is not None:
        require_non_negative(max_doc_frequency, field_name="max_doc_frequency", error_cls=error_cls)
    if min_doc_frequency is not None and max_doc_frequency is not None:
        require_ordered_bounds(
            min_doc_frequency,
            max_doc_frequency,
            lower_name="min_doc_frequency",
            upper_name="max_doc_frequency",
            error_cls=error_cls,
        )
    if min_word_length is not None:
        require_non_negative(min_word_length, field_name="min_word_length", error_cls=error_cls)
    if max_word_length is not None:
        require_non_negative(max_word_length, field_name="max_word_length", error_cls=error_cls)
    if min_word_length is not None and max_word_length is not None:
        require_ordered_bounds(
            min_word_length,
            max_word_length,
            lower_name="min_word_length",
            upper_name="max_word_length",
            error_cls=error_cls,
        )
    if boost_factor is not None:
        require_non_negative(boost_factor, field_name="boost_factor", error_cls=error_cls)
    if stopwords is not None:
        require_non_empty_strings(stopwords, field_name="stopwords", error_cls=error_cls)

    named_options: list[tuple[str, Any]] = []
    if min_term_frequency is not None:
        named_options.append(("min_term_frequency", min_term_frequency))
    if max_query_terms is not None:
        named_options.append(("max_query_terms", max_query_terms))
    if min_doc_frequency is not None:
        named_options.append(("min_doc_frequency", min_doc_frequency))
    if max_doc_frequency is not None:
        named_options.append(("max_doc_frequency", max_doc_frequency))
    if min_word_length is not None:
        named_options.append(("min_word_length", min_word_length))
    if max_word_length is not None:
        named_options.append(("max_word_length", max_word_length))
    if boost_factor is not None:
        named_options.append(("boost_factor", boost_factor))
    if stopwords is not None:
        named_options.append(("stopwords", array(stopwords, type_=Text())))

    def _build_mlt_call(source_arg: ClauseElement, *, include_fields: bool) -> ClauseElement:
        positional_args: list[ClauseElement] = [source_arg]
        if include_fields and fields is not None:
            positional_args.append(array(fields, type_=Text()))
        return PDBFunctionWithNamedArgs("more_like_this", positional_args, named_options)

    if document_ids is not None:
        clauses = [
            field.operate(_QUERY, _build_mlt_call(literal(doc_id), include_fields=True)) for doc_id in document_ids
        ]
        return or_(*clauses)

    if document_id is not None:
        return field.operate(_QUERY, _build_mlt_call(literal(document_id), include_fields=True))

    payload = document if isinstance(document, str) else json.dumps(document, separators=(",", ":"), sort_keys=True)
    return field.operate(_QUERY, _build_mlt_call(literal(payload), include_fields=False))
