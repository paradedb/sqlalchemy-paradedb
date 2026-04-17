from __future__ import annotations

from sqlalchemy import func, literal
from sqlalchemy.sql.elements import ClauseElement


def json_text(json_expr: ClauseElement, key: str) -> ClauseElement:
    return json_expr.op("->>")(literal(key))


def concat_ws(separator: str, *parts: ClauseElement) -> ClauseElement:
    return func.concat_ws(separator, *parts)
