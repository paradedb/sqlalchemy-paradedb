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

from paradedb.sqlalchemy.tokenizer import TokenizerSpec

from ._select_introspection import has_limit, has_order_by
from ._pdb_cast import PDBCast
from .errors import (
    DuplicateTokenizerAliasError,
    FieldNotIndexedError,
    InvalidArgumentError,
    InvalidBM25FieldError,
    InvalidKeyFieldError,
    MissingKeyFieldError,
)


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
        if isinstance(element.expr, PDBCast) or _bm25_field_name(element) is None:
            return f"({expr_sql})"
        return expr_sql
    return f"(({expr_sql})::{element.tokenizer.render()})"


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
        if not isinstance(expr, BM25Field):
            continue
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

    field_names = {_bm25_field_name(expr) for expr in index.expressions if isinstance(expr, BM25Field)}
    if key_field not in field_names:
        raise InvalidKeyFieldError(f"BM25 key_field '{key_field}' must match one of the indexed BM25Field columns")

    first_field = index.expressions[0]
    if not isinstance(first_field, BM25Field):
        raise InvalidBM25FieldError("BM25 indexes must use BM25Field for every indexed field")
    first_field_name = _bm25_field_name(first_field)
    if first_field_name != key_field:
        raise InvalidKeyFieldError(f"BM25 key_field '{key_field}' must be the first indexed BM25Field")
    if first_field.tokenizer is not None:
        raise InvalidKeyFieldError(f"BM25 key_field '{key_field}' must be untokenized")


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
_TOKENIZER_NAME_RE = re.compile(r"::pdb\.([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    expr = _strip_outer_parens(field_expr.strip())
    cast_marker = re.search(r"::\s*pdb\.", expr, re.IGNORECASE)
    if cast_marker is not None:
        expr = _strip_outer_parens(expr[: cast_marker.start()].strip())

    if "->" in expr:
        expr = _strip_outer_parens(expr.split("->", 1)[0].strip())

    # Strip schema/table qualifiers and keep the terminal identifier.
    if "." in expr:
        expr = _strip_outer_parens(expr.rsplit(".", 1)[1].strip())

    if expr.startswith('"') and expr.endswith('"') and len(expr) >= 2:
        return expr[1:-1].replace('""', '"')
    if _IDENT_RE.match(expr):
        return expr
    return None


def _strip_outer_parens(value: str) -> str:
    """Strip matching outer parentheses from a string."""
    expr = value
    while expr.startswith("(") and expr.endswith(")") and _has_balanced_outer_parens(expr):
        expr = expr[1:-1].strip()
    return expr


def _has_balanced_outer_parens(value: str) -> bool:
    depth = 0
    in_single = False
    in_double = False

    for i, ch in enumerate(value):
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if in_single or in_double:
            continue

        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and i != len(value) - 1:
                return False
            if depth < 0:
                return False
    return depth == 0


def _extract_key_field(indexdef: str) -> str | None:
    match = _KEY_FIELD_RE.search(indexdef)
    if match:
        return match.group(1)
    return None


_WHERE_CLAUSE_RE = re.compile(r"\bWHERE\s*\((.+)\)\s*$", re.IGNORECASE)


def _extract_where_clause(indexdef: str) -> str | None:
    """Extract the WHERE predicate from a ``pg_indexes.indexdef`` string.

    PostgreSQL normalizes partial index predicates as ``WHERE (<condition>)``.
    Returns the inner condition text or ``None`` if not present.
    """
    match = _WHERE_CLAUSE_RE.search(indexdef)
    if match:
        return match.group(1).strip()
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


def _current_schema_name(conn) -> str:
    row = conn.execute(text("SELECT current_schema()")).one()
    return str(row[0])


def _normalize_reloption_value(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip()
    if len(v) >= 2 and v[0] == "'" and v[-1] == "'":
        return v[1:-1].replace("''", "'")
    return v


def _introspect_bm25_index_rows(conn, *, schema_name: str, table_name: str | None = None):
    return (
        conn.execute(
            text(
                """
            SELECT
              ns.nspname AS schemaname,
              tbl.relname AS tablename,
              idx.relname AS indexname,
              pg_get_indexdef(idx.oid) AS indexdef,
              split_part(opt.opt, '=', 2) AS key_field,
              key_ord.ord::int AS ordinality,
              pg_get_indexdef(idx.oid, key_ord.ord::int, true) AS keydef,
              CASE WHEN key_ord.attnum > 0 THEN attr.attname ELSE NULL END AS attname
            FROM pg_class AS idx
            JOIN pg_namespace AS ns ON ns.oid = idx.relnamespace
            JOIN pg_index AS i ON i.indexrelid = idx.oid
            JOIN pg_class AS tbl ON tbl.oid = i.indrelid
            LEFT JOIN LATERAL (
              SELECT opt
              FROM unnest(COALESCE(idx.reloptions, ARRAY[]::text[])) AS opt
              WHERE split_part(opt, '=', 1) = 'key_field'
              LIMIT 1
            ) AS opt ON true
            JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS key_ord(attnum, ord) ON true
            LEFT JOIN pg_attribute AS attr
              ON attr.attrelid = tbl.oid
             AND attr.attnum = key_ord.attnum
            WHERE ns.nspname = :schema_name
              AND (CAST(:table_name AS text) IS NULL OR tbl.relname = CAST(:table_name AS text))
              AND pg_get_indexdef(idx.oid) ILIKE '%USING bm25%'
            ORDER BY idx.relname, key_ord.ord
            """
            ),
            {"schema_name": schema_name, "table_name": table_name},
        )
        .mappings()
        .all()
    )


def describe(engine: Engine, table, *, schema: str | None = None) -> list[IndexMeta]:
    table_schema = schema if schema is not None else getattr(table, "schema", None)
    with engine.connect() as conn:
        effective_schema = table_schema if table_schema is not None else _current_schema_name(conn)
        rows = _introspect_bm25_index_rows(
            conn,
            schema_name=effective_schema,
            table_name=table.name,
        )

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        index_name = str(row["indexname"])
        group = grouped.setdefault(
            index_name,
            {
                "indexdef": row["indexdef"],
                "key_field": _normalize_reloption_value(row["key_field"]),
                "fields": [],
                "aliases": {},
                "tokenizers": {},
            },
        )

        key_field = _normalize_reloption_value(row["key_field"])
        if key_field and not group["key_field"]:
            group["key_field"] = key_field

        raw_expr = str(row["keydef"] or "")
        field_name = row["attname"] or _extract_field_name(raw_expr)
        if field_name is None:
            continue

        fields = group["fields"]
        if field_name not in fields:
            fields.append(field_name)

        alias = _extract_alias(raw_expr)
        if alias is not None:
            aliases = group["aliases"]
            aliases[alias] = field_name

        tokenizer = _extract_tokenizer_name(raw_expr)
        if tokenizer is not None:
            tokenizers = group["tokenizers"]
            tokenizers.setdefault(field_name, []).append(tokenizer)

    output: list[IndexMeta] = []
    for index_name, data in grouped.items():
        key_field = data["key_field"] or _extract_key_field(str(data["indexdef"]))
        output.append(
            IndexMeta(
                index_name=index_name,
                key_field=key_field,
                fields=tuple(data["fields"]),
                aliases=dict(data["aliases"]),
                tokenizers={k: tuple(v) for k, v in data["tokenizers"].items()},
            )
        )

    output.sort(key=lambda meta: meta.index_name)
    return output


def assert_indexed(
    engine: Engine,
    column: Any,
    *,
    tokenizer: str | None = None,
    schema: str | None = None,
) -> None:
    """Raise :exc:`FieldNotIndexedError` if *column* is not covered by any BM25 index.

    Args:
        engine: SQLAlchemy engine connected to the ParadeDB database.
        column: A table-bound column expression (e.g. ``Product.description``).
        tokenizer: Optional tokenizer name to verify, e.g. ``"literal"`` or
                   ``"unicode_words"``.  When given, raises if the column is not
                   indexed with that specific tokenizer.
        schema: Optional schema override. Defaults to ``column.table.schema`` when set,
                otherwise the connection's ``current_schema()``.

    Example::

        assert_indexed(engine, Product.category, tokenizer="literal")
    """
    table = getattr(column, "table", None)
    if table is None:
        raise InvalidArgumentError("column must be a table-bound column expression")
    col_name: str | None = getattr(column, "name", None)
    if col_name is None:
        raise InvalidArgumentError("column must have a name attribute")

    for idx_meta in describe(engine, table, schema=schema):
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
        warnings.append("No WHERE clause found; query will perform a full table scan without ParadeDB")
    elif not _inspect.has_paradedb_predicate(whereclause):
        warnings.append("No ParadeDB predicate found in WHERE clause; query will not use a BM25 index")

    if has_order_by(stmt) and not has_limit(stmt):
        warnings.append("ORDER BY is present without LIMIT; Top K pushdown to ParadeDB requires both")

    return warnings
