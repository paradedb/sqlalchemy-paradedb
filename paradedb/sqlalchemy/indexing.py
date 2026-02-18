from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Index, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import CompileError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ClauseElement, ColumnElement
from sqlalchemy.sql.visitors import InternalTraversal

from .errors import (
    DuplicateTokenizerAliasError,
    FieldNotIndexedError,
    InvalidArgumentError,
    InvalidBM25FieldError,
    InvalidKeyFieldError,
    MissingKeyFieldError,
)


@dataclass(frozen=True)
class TokenizerSpec:
    name: str | None = None
    options: tuple[tuple[str, Any], ...] = ()
    raw_sql: str | None = None
    alias: str | None = None

    def render(self) -> str:
        if self.raw_sql is not None:
            return self.raw_sql

        if self.name is None:
            raise InvalidArgumentError("tokenizer name is required unless raw_sql is provided")

        if not self.options:
            return f"pdb.{self.name}"

        rendered_options = ",".join(f"{key}={_format_option_value(value)}" for key, value in self.options)
        escaped = rendered_options.replace("'", "''")
        return f"pdb.{self.name}('{escaped}')"


def _format_option_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _build_spec(name: str, *, alias: str | None = None, **kwargs: Any) -> TokenizerSpec:
    options: dict[str, Any] = {key: value for key, value in kwargs.items() if value is not None}
    if alias is not None:
        options["alias"] = alias
    return TokenizerSpec(name=name, options=tuple(sorted(options.items())), alias=alias)


def unicode(*, alias: str | None = None, lowercase: bool | None = None, stemmer: str | None = None) -> TokenizerSpec:
    # ParadeDB currently exposes this tokenizer as `unicode_words`.
    return _build_spec("unicode_words", alias=alias, lowercase=lowercase, stemmer=stemmer)


def literal(*, alias: str | None = None) -> TokenizerSpec:
    return _build_spec("literal", alias=alias)


def literal_normalized(*, alias: str | None = None) -> TokenizerSpec:
    return _build_spec("literal_normalized", alias=alias)


def ngram(
    *,
    alias: str | None = None,
    min_gram: int | None = None,
    max_gram: int | None = None,
    prefix_only: bool | None = None,
) -> TokenizerSpec:
    return _build_spec(
        "ngram",
        alias=alias,
        min_gram=min_gram,
        max_gram=max_gram,
        prefix_only=prefix_only,
    )


def raw(sql: str, *, alias: str | None = None) -> TokenizerSpec:
    return TokenizerSpec(raw_sql=sql, alias=alias)


class _TokenizeNamespace:
    unicode = staticmethod(unicode)
    literal = staticmethod(literal)
    literal_normalized = staticmethod(literal_normalized)
    ngram = staticmethod(ngram)
    raw = staticmethod(raw)


tokenize = _TokenizeNamespace()


class BM25Field(ColumnElement[Any]):
    """Represents a ParadeDB BM25 index field expression."""

    inherit_cache = True
    _traverse_internals = [
        ("expr", InternalTraversal.dp_clauseelement),
        ("tokenizer", InternalTraversal.dp_plain_obj),
    ]

    def __init__(self, expr: ClauseElement, *, tokenizer: TokenizerSpec | None = None) -> None:
        self.expr = expr
        self.tokenizer = tokenizer

    @property
    def table(self):  # pragma: no cover - SQLAlchemy internals may use this dynamically
        return getattr(self.expr, "table", None)


@compiles(BM25Field, "postgresql")
def _compile_bm25_field(element: BM25Field, compiler, **kw: Any) -> str:
    expr_sql = compiler.process(element.expr, **kw)
    if element.tokenizer is None:
        return expr_sql
    return f"({expr_sql}::{element.tokenizer.render()})"


@compiles(BM25Field)
def _compile_bm25_field_default(element: BM25Field, compiler, **kw: Any) -> str:
    raise CompileError("BM25Field is only supported for PostgreSQL dialects")


def _is_bm25_index(index: Index) -> bool:
    using = index.dialect_options["postgresql"].get("using")
    return bool(using and str(using).lower() == "bm25")


def _bm25_field_name(field: BM25Field) -> str | None:
    return getattr(getattr(field, "expr", None), "name", None)


def validate_bm25_index(index: Index) -> None:
    if not _is_bm25_index(index):
        return

    if not index.expressions:
        raise InvalidBM25FieldError("BM25 indexes must include at least one BM25Field")

    if not all(isinstance(expr, BM25Field) for expr in index.expressions):
        raise InvalidBM25FieldError("BM25 indexes must use BM25Field for every indexed field")

    aliases: set[str] = set()
    for expr in index.expressions:
        tokenizer = expr.tokenizer
        if tokenizer is None or tokenizer.alias is None:
            continue
        if tokenizer.alias in aliases:
            raise DuplicateTokenizerAliasError(f"Duplicate tokenizer alias '{tokenizer.alias}' in BM25 index")
        aliases.add(tokenizer.alias)

    with_options = index.dialect_options["postgresql"].get("with") or {}
    key_field = with_options.get("key_field")
    if not key_field:
        raise MissingKeyFieldError("BM25 indexes require postgresql_with={'key_field': '<column>'}")

    field_names = {_bm25_field_name(expr) for expr in index.expressions}
    if key_field not in field_names:
        raise InvalidKeyFieldError(f"BM25 key_field '{key_field}' must match one of the indexed BM25Field columns")


@event.listens_for(Index, "before_create")
def _validate_bm25_before_create(index: Index, connection, **kw: Any) -> None:
    validate_bm25_index(index)


@dataclass(frozen=True)
class IndexMeta:
    index_name: str
    key_field: str | None
    fields: tuple[str, ...]
    aliases: dict[str, str]
    tokenizers: dict[str, tuple[str, ...]] = field(default_factory=dict)
    """Maps field name to the tokenizer names used in this index, e.g. ``{"description": ("unicode_words",)}``."""


_KEY_FIELD_RE = re.compile(r"key_field\s*=\s*'?\"?([^'\",)\s]+)\"?'?", re.IGNORECASE)
_ALIAS_RE = re.compile(r"alias\s*=\s*([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
_CAST_FIELD_RE = re.compile(r"^\(*\"?([A-Za-z_][A-Za-z0-9_]*)\"?\)*\s*::\s*pdb\.", re.IGNORECASE)
_PLAIN_FIELD_RE = re.compile(r'^\(*"?([A-Za-z_][A-Za-z0-9_]*)"?\)*$')
_TOKENIZER_NAME_RE = re.compile(r"::pdb\.([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)


def _split_top_level_csv(expr: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False

    for ch in expr:
        if ch == "'" and not in_double:
            in_single = not in_single
            current.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            current.append(ch)
            continue
        if not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == "," and depth == 0:
                piece = "".join(current).strip()
                if piece:
                    parts.append(piece)
                current = []
                continue
        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _extract_bm25_field_list(indexdef: str) -> list[str]:
    marker = re.search(r"USING\s+bm25\s*\(", indexdef, re.IGNORECASE)
    if marker is None:
        return []

    start = marker.end()
    depth = 1
    in_single = False
    in_double = False
    i = start
    while i < len(indexdef):
        ch = indexdef[i]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return _split_top_level_csv(indexdef[start:i])
        i += 1
    return []


def _extract_field_name(field_expr: str) -> str | None:
    expr = field_expr.strip()
    cast_match = _CAST_FIELD_RE.match(expr)
    if cast_match:
        return cast_match.group(1)
    plain_match = _PLAIN_FIELD_RE.match(expr)
    if plain_match:
        return plain_match.group(1)
    return None


def _extract_key_field(indexdef: str) -> str | None:
    match = _KEY_FIELD_RE.search(indexdef)
    if match:
        return match.group(1)
    return None


def _extract_alias(index_expr: str) -> str | None:
    match = _ALIAS_RE.search(index_expr)
    if match:
        return match.group(1)
    return None


def _extract_tokenizer_name(field_expr: str) -> str | None:
    """Return the bare tokenizer name from a field expression, e.g. ``unicode_words`` from
    ``(description::pdb.unicode_words('lowercase=true'))``. Returns ``None`` for plain fields."""
    match = _TOKENIZER_NAME_RE.search(field_expr)
    return match.group(1) if match else None


def describe(engine: Engine, table) -> list[IndexMeta]:
    query = text(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = :table_name
          AND indexdef ILIKE '%USING bm25%'
        ORDER BY indexname
        """
    )

    rows = engine.connect().execute(query, {"table_name": table.name}).fetchall()
    output: list[IndexMeta] = []
    for row in rows:
        indexdef: str = row.indexdef
        key_field = _extract_key_field(indexdef)
        raw_fields = _extract_bm25_field_list(indexdef)
        aliases: dict[str, str] = {}
        tokenizer_map: dict[str, list[str]] = {}
        fields_ordered: list[str] = []
        for raw in raw_fields:
            field_name = _extract_field_name(raw)
            if field_name is None:
                continue
            if field_name not in fields_ordered:
                fields_ordered.append(field_name)
            alias = _extract_alias(raw)
            if alias is not None:
                aliases[alias] = field_name
            tok = _extract_tokenizer_name(raw)
            if tok is not None:
                tokenizer_map.setdefault(field_name, []).append(tok)

        output.append(
            IndexMeta(
                index_name=row.indexname,
                key_field=key_field,
                fields=tuple(fields_ordered),
                aliases=aliases,
                tokenizers={k: tuple(v) for k, v in tokenizer_map.items()},
            )
        )
    return output


def assert_indexed(
    engine: Engine,
    column: Any,
    *,
    tokenizer: str | None = None,
) -> None:
    """Raise :exc:`FieldNotIndexedError` if *column* is not covered by any BM25 index.

    Args:
        engine: SQLAlchemy engine connected to the ParadeDB database.
        column: A table-bound column expression (e.g. ``Product.description``).
        tokenizer: Optional tokenizer name to verify, e.g. ``"literal"`` or
                   ``"unicode_words"``.  When given, raises if the column is not
                   indexed with that specific tokenizer.

    Example::

        assert_indexed(engine, Product.category, tokenizer="literal")
    """
    table = getattr(column, "table", None)
    if table is None:
        raise InvalidArgumentError("column must be a table-bound column expression")
    col_name: str | None = getattr(column, "name", None)
    if col_name is None:
        raise InvalidArgumentError("column must have a name attribute")

    for idx_meta in describe(engine, table):
        if col_name not in idx_meta.fields:
            continue
        if tokenizer is None:
            return  # field is indexed; no tokenizer constraint
        if tokenizer in idx_meta.tokenizers.get(col_name, ()):
            return  # field is indexed with the requested tokenizer

    msg = f"'{col_name}' is not indexed in any BM25 index on '{table.name}'"
    if tokenizer:
        msg += f" with tokenizer '{tokenizer}'"
    raise FieldNotIndexedError(msg)


def validate_pushdown(stmt: Any) -> list[str]:
    """Inspect *stmt* for patterns that will not push down to ParadeDB.

    Performs **static AST analysis only** — no database connection is required.
    Returns a (possibly empty) list of human-readable warning strings.

    Example::

        issues = validate_pushdown(stmt)
        for w in issues:
            print("Warning:", w)
    """
    from . import inspect as _inspect

    warnings: list[str] = []

    whereclause = getattr(stmt, "whereclause", None)
    if whereclause is None:
        warnings.append(
            "No WHERE clause found; query will perform a full table scan without ParadeDB"
        )
    elif not _inspect.has_paradedb_predicate(whereclause):
        warnings.append(
            "No ParadeDB predicate found in WHERE clause; query will not use a BM25 index"
        )

    order_by = getattr(stmt, "_order_by_clauses", None) or ()
    limit = getattr(stmt, "_limit_clause", None)
    if order_by and limit is None:
        warnings.append(
            "ORDER BY is present without LIMIT; top-N pushdown to ParadeDB requires both"
        )

    return warnings
