from __future__ import annotations

from sqlalchemy import func, literal
from sqlalchemy.sql.elements import ClauseElement

from .indexing import TokenizerSpec


def json_text(json_expr: ClauseElement, key: str) -> ClauseElement:
    return json_expr.op("->>")(literal(key))


def concat_ws(separator: str, *parts: ClauseElement) -> ClauseElement:
    return func.concat_ws(separator, *parts)


def tokenizer_alias(tokenizer: TokenizerSpec | None) -> str | None:
    return None if tokenizer is None else tokenizer.alias
