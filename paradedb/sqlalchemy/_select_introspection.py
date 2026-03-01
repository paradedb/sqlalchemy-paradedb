from __future__ import annotations

import re
from typing import Any

_ORDER_BY_RE = re.compile(r"\border\s+by\b", re.IGNORECASE)
_LIMIT_RE = re.compile(r"\blimit\b", re.IGNORECASE)


def has_order_by(stmt: Any) -> bool:
    getter = getattr(stmt, "get_order_by", None)
    if callable(getter):
        clauses = tuple(getter())
        if clauses:
            return True

    order_by_clause = getattr(stmt, "order_by_clause", None)
    if order_by_clause is not None:
        clauses = getattr(order_by_clause, "clauses", None)
        if clauses is not None:
            return bool(tuple(clauses))
        return bool(str(order_by_clause).strip())

    return bool(_ORDER_BY_RE.search(str(stmt)))


def has_limit(stmt: Any) -> bool:
    getter = getattr(stmt, "get_limit", None)
    if callable(getter):
        if getter() is not None:
            return True

    limit_clause = getattr(stmt, "limit_clause", None)
    if limit_clause is not None:
        return True

    return bool(_LIMIT_RE.search(str(stmt)))
