from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Index, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import CompileError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ClauseElement, ColumnElement
from sqlalchemy.sql.visitors import InternalTraversal


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
            raise ValueError("tokenizer name is required unless raw_sql is provided")

        if not self.options:
            return f"pdb.{self.name}()"

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
        raise ValueError("BM25 indexes must include at least one BM25Field")

    if not all(isinstance(expr, BM25Field) for expr in index.expressions):
        raise ValueError("BM25 indexes must use BM25Field for every indexed field")

    aliases: set[str] = set()
    for expr in index.expressions:
        tokenizer = expr.tokenizer
        if tokenizer is None or tokenizer.alias is None:
            continue
        if tokenizer.alias in aliases:
            raise ValueError(f"Duplicate tokenizer alias '{tokenizer.alias}' in BM25 index")
        aliases.add(tokenizer.alias)

    with_options = index.dialect_options["postgresql"].get("with") or {}
    key_field = with_options.get("key_field")
    if not key_field:
        raise ValueError("BM25 indexes require postgresql_with={'key_field': '<column>'}")

    field_names = {_bm25_field_name(expr) for expr in index.expressions}
    if key_field not in field_names:
        raise ValueError(f"BM25 key_field '{key_field}' must match one of the indexed BM25Field columns")


@event.listens_for(Index, "before_create")
def _validate_bm25_before_create(index: Index, connection, **kw: Any) -> None:
    validate_bm25_index(index)


@dataclass(frozen=True)
class IndexMeta:
    index_name: str
    key_field: str | None
    fields: tuple[str, ...]
    aliases: dict[str, str]


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
        key_field: str | None = None
        marker = "key_field='"
        marker_idx = indexdef.find(marker)
        if marker_idx != -1:
            key_start = marker_idx + len(marker)
            key_end = indexdef.find("'", key_start)
            if key_end != -1:
                key_field = indexdef[key_start:key_end]

        output.append(
            IndexMeta(
                index_name=row.indexname,
                key_field=key_field,
                fields=(),
                aliases={},
            )
        )
    return output
