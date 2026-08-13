from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Text, cast, func, literal, literal_column
from sqlalchemy.dialects.postgresql import ARRAY, array
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import ClauseElement, ColumnElement

from paradedb.sqlalchemy.tokenizer import Tokenizer

from ._functions import PDBFunctionWithNamedArgs
from ._pdb_cast import PDBCast
from .errors import InvalidArgumentError, InvalidMoreLikeThisOptionsError
from .validation import (
    require_non_empty_sequence,
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
_PROXIMITY: Any = operators.custom_op("##", precedence=5)
_PROXIMITY_ORDERED: Any = operators.custom_op("##>", precedence=5)
_PDB_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TextClause = str | ClauseElement
_SearchValue = _TextClause | Sequence[_TextClause]
_TOKENIZER_PARAMS = Sequence[Any]


def _text_literal(value: str) -> ClauseElement:
    return literal(value, type_=Text())


def _to_text_clause(value: _TextClause) -> ClauseElement:
    if isinstance(value, ClauseElement):
        return value
    return _text_literal(value)


def _text_array(values: Sequence[_TextClause]) -> ClauseElement:
    return array([_to_text_clause(value) for value in values])


def _inline_string_literal(value: str) -> ClauseElement:
    return literal_column("'" + value.replace("'", "''") + "'", Text())


def _to_search_value(value: _SearchValue) -> ClauseElement:
    if isinstance(value, (str, ClauseElement)):
        return _to_text_clause(value)
    if not isinstance(value, Sequence):
        raise InvalidArgumentError("value must be a string, SQL expression, or a sequence of those values")
    if not value:
        raise InvalidArgumentError("value must contain at least one token")
    return _text_array(value)


def boost(value: _SearchValue, factor: float) -> ClauseElement:
    return PDBCast(_to_search_value(value), "boost", (factor,))


def constant(value: _SearchValue, score: float) -> ClauseElement:
    expr = _to_search_value(value)
    if isinstance(expr, PDBCast) and expr.type_name in {"fuzzy", "slop"}:
        expr = PDBCast(expr, "query")
    return PDBCast(expr, "const", (score,))


def slop(value: _SearchValue, n: int) -> ClauseElement:
    require_non_negative(n, field_name="slop")
    expr = _to_search_value(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, ClauseElement)):
        expr = cast(expr, ARRAY(Text()))
    return PDBCast(expr, "slop", (n,))


def fuzzy(
    value: _SearchValue,
    distance: int,
    prefix: bool | None = None,
    transpose_cost_one: bool | None = None,
) -> ClauseElement:
    if distance < 0 or distance > 2:
        raise InvalidArgumentError("distance must be between 0 and 2")
    args: list[object] = [distance]
    if prefix is not None:
        args.append(prefix)
    if transpose_cost_one is not None:
        if prefix is None:
            args.append(False)
        args.append(transpose_cost_one)
    return PDBCast(_to_search_value(value), "fuzzy", args)


def tokenize(value: _SearchValue, tokenizer: Tokenizer) -> ClauseElement:
    return PDBCast(_to_search_value(value), None, raw_cast=tokenizer.render())


def match_all(
    field: ColumnElement,
    value: _SearchValue,
) -> ColumnElement[bool]:
    return field.operate(_MATCH_ALL, _to_search_value(value))


def match_any(
    field: ColumnElement,
    value: _SearchValue,
) -> ColumnElement[bool]:
    return field.operate(_MATCH_ANY, _to_search_value(value))


def term(
    field: ColumnElement,
    value: _SearchValue,
) -> ColumnElement[bool]:
    return field.operate(_TERM, _to_search_value(value))


def phrase(
    field: ColumnElement,
    value: _SearchValue,
) -> ColumnElement[bool]:
    return field.operate(_PHRASE, _to_search_value(value))


def regex(
    field: ColumnElement,
    pattern: _TextClause,
) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.regex(_to_text_clause(pattern)))


def all(field: ColumnElement) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.all())


def exists(field: ColumnElement) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.exists())


class ProximityExpr:
    def __init__(self, expr: ClauseElement) -> None:
        self.expr = expr

    def within(
        self,
        distance: int,
        other: str | ProximityExpr | ClauseElement,
        ordered: bool = False,
    ) -> ProximityExpr:
        return ProximityExpr(_proximity_chain(self.expr, other, distance=distance, ordered=ordered))


def prox_regex(pattern: _TextClause, max_expansions: int | None = None) -> ProximityExpr:
    if max_expansions is not None:
        require_non_negative(max_expansions, field_name="max_expansions")
        return ProximityExpr(func.pdb.prox_regex(_to_text_clause(pattern), max_expansions))
    return ProximityExpr(func.pdb.prox_regex(_to_text_clause(pattern)))


def prox_array(*clauses: _TextClause | ProximityExpr) -> ProximityExpr:
    if not clauses:
        raise InvalidArgumentError("prox_array requires at least one clause")
    casted_clauses = [_to_proximity_operand(clause) for clause in clauses]
    return ProximityExpr(func.pdb.prox_array(*casted_clauses))


def prox_str(clause: _TextClause) -> ProximityExpr:
    return ProximityExpr(_to_proximity_operand(clause))


def proximity(field: ColumnElement, prox: ProximityExpr | ClauseElement) -> ColumnElement[bool]:
    prox_expr = prox.expr if isinstance(prox, ProximityExpr) else prox
    return field.operate(_QUERY, prox_expr)


def _to_proximity_operand(value: str | ClauseElement | ProximityExpr) -> ClauseElement:
    if isinstance(value, ProximityExpr):
        return value.expr
    if isinstance(value, str):
        return _inline_string_literal(value)
    return value


def _proximity_chain(
    left: str | ClauseElement | ProximityExpr,
    right: str | ClauseElement | ProximityExpr,
    *,
    distance: int,
    ordered: bool = False,
) -> ClauseElement:
    require_non_negative(distance, field_name="distance")
    left_expr = _to_proximity_operand(left)
    right_expr = _to_proximity_operand(right)
    op = _PROXIMITY_ORDERED if ordered else _PROXIMITY
    return left_expr.operate(op, literal(distance)).operate(op, right_expr)


def parse(
    field: ColumnElement, query: _TextClause, *, lenient: bool = False, conjunction_mode: bool = False
) -> ColumnElement[bool]:
    return field.operate(_QUERY, func.pdb.parse(_to_text_clause(query), lenient, conjunction_mode))


def phrase_prefix(
    field: ColumnElement,
    terms: list[_TextClause],
    *,
    max_expansions: int | None = None,
) -> ColumnElement[bool]:
    if not terms:
        raise InvalidArgumentError("phrase_prefix requires at least one term")
    if max_expansions is not None:
        require_positive(max_expansions, field_name="max_expansions")
        return field.operate(_QUERY, func.pdb.phrase_prefix(_text_array(terms), max_expansions))
    else:
        return field.operate(_QUERY, func.pdb.phrase_prefix(_text_array(terms)))


def regex_phrase(
    field: ColumnElement,
    terms: list[_TextClause],
    *,
    slop: int = 0,
    max_expansions: int | None = None,
) -> ColumnElement[bool]:
    if not terms:
        raise InvalidArgumentError("regex_phrase requires at least one term")
    require_non_negative(slop, field_name="slop")
    if max_expansions is not None:
        require_positive(max_expansions, field_name="max_expansions")
        return field.operate(_QUERY, func.pdb.regex_phrase(_text_array(terms), slop, max_expansions))
    else:
        return field.operate(_QUERY, func.pdb.regex_phrase(_text_array(terms), slop))


def range_term(
    field: ColumnElement,
    bounds: str | ClauseElement | int | float,
    *,
    relation: str = "Intersects",
    range_type: str | None = None,
) -> ColumnElement[bool]:
    """Match rows where a range-typed field satisfies a range predicate.

    Args:
        field: A range-typed column (int4range, daterange, tstzrange, etc.).
        bounds: Either a scalar point (e.g. ``1``) or a range literal string
                (e.g. ``"[3,9]"``, ``"(3,9]"``).
        relation: One of ``"Intersects"``, ``"Contains"``, ``"Within"``,
                  ``"ContainsOrIntersects"``. Defaults to ``"Intersects"``.
                  Only used when ``bounds`` is a range literal string.
        range_type: Optional PostgreSQL range type for explicit casting, e.g.
                    ``"int4range"``, ``"int8range"``, ``"numrange"``,
                    ``"daterange"``, ``"tsrange"``, ``"tstzrange"``.
                    When provided, generates ``'bounds'::range_type`` cast.
                    Only used when ``bounds`` is a range literal string.

    Generates::

        field @@@ pdb.range_term(1)
        field @@@ pdb.range_term('[3,9]', 'Contains')
        field @@@ pdb.range_term('[3,9]'::int4range, 'Contains')
    """
    if not isinstance(bounds, str):
        if relation != "Intersects":
            raise InvalidArgumentError("relation is only supported when bounds is a range literal string")
        if range_type is not None:
            raise InvalidArgumentError("range_type is only supported when bounds is a range literal string")
        scalar_arg = bounds if isinstance(bounds, ClauseElement) else literal(bounds)
        return field.operate(_QUERY, func.pdb.range_term(scalar_arg))

    if relation not in _VALID_RANGE_RELATIONS:
        raise InvalidArgumentError(f"relation must be one of: {', '.join(sorted(_VALID_RANGE_RELATIONS))}")
    escaped_relation = relation.replace("'", "''")
    relation_arg: ClauseElement = literal_column(f"'{escaped_relation}'")
    if range_type is not None:
        if range_type not in _VALID_RANGE_TYPES:
            raise InvalidArgumentError(f"range_type must be one of: {', '.join(sorted(_VALID_RANGE_TYPES))}")
        escaped = bounds.replace("'", "''")
        range_bounds_arg: ClauseElement = literal_column(f"'{escaped}'::{range_type}")
    else:
        range_bounds_arg = _text_literal(bounds)
    return field.operate(_QUERY, func.pdb.range_term(range_bounds_arg, relation_arg))


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
    error_cls = InvalidMoreLikeThisOptionsError

    sources_provided = sum(x is not None for x in (document_id, document))
    if sources_provided != 1:
        raise error_cls("exactly one of document_id, or document must be provided")
    if document is not None and fields is not None:
        raise error_cls("fields can only be used with document_id")
    if fields is not None:
        require_non_empty_sequence(fields, field_name="fields", error_cls=error_cls)
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
        named_options.append(("stopwords", _text_array(stopwords)))

    def _build_mlt_call(source_arg: ClauseElement, *, include_fields: bool) -> ClauseElement:
        positional_args: list[ClauseElement] = [source_arg]
        if include_fields and fields is not None:
            positional_args.append(_text_array(fields))
        return PDBFunctionWithNamedArgs("more_like_this", positional_args, named_options)

    if document_id is not None:
        return field.operate(_QUERY, _build_mlt_call(literal(document_id), include_fields=True))

    payload = document if isinstance(document, str) else json.dumps(document, separators=(",", ":"), sort_keys=True)
    payload_arg = _text_literal(payload) if isinstance(payload, str) else literal(payload)
    return field.operate(_QUERY, _build_mlt_call(payload_arg, include_fields=False))
