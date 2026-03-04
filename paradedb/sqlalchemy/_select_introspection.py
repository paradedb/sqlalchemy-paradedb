from __future__ import annotations

from typing import Any


def has_order_by(stmt: Any) -> bool:
    clauses = getattr(stmt, "_order_by_clauses", None)
    if clauses is not None:
        return bool(tuple(clauses))

    getter = getattr(stmt, "get_order_by", None)
    if callable(getter):
        clauses = tuple(getter())
        if clauses:
            return True

    order_by_clause = getattr(stmt, "order_by_clause", None)
    if order_by_clause is not None:
        order_by_clauses = getattr(order_by_clause, "clauses", None)
        if order_by_clauses is not None:
            return bool(tuple(order_by_clauses))
        return bool(str(order_by_clause).strip())

    return False


def has_limit(stmt: Any) -> bool:
    if getattr(stmt, "_limit_clause", None) is not None:
        return True

    getter = getattr(stmt, "get_limit", None)
    if callable(getter):
        if getter() is not None:
            return True

    limit_clause = getattr(stmt, "limit_clause", None)
    if limit_clause is not None:
        return True

    return False
