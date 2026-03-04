from __future__ import annotations

from typing import Any


def _has_non_empty_clauses(clause_list: Any) -> bool:
    clauses = getattr(clause_list, "clauses", None)
    if clauses is not None:
        return bool(tuple(clauses))
    return False


def has_order_by(stmt: Any) -> bool:
    # SQLAlchemy 2.x exposes a ClauseList here. It is private, but this shape is
    # less version-coupled than reaching directly for `_order_by_clauses`.
    order_by_clause = getattr(stmt, "_order_by_clause", None)
    if order_by_clause is not None and _has_non_empty_clauses(order_by_clause):
        return True

    clauses = getattr(stmt, "_order_by_clauses", None)
    if clauses is not None and bool(tuple(clauses)):
        return True

    return False


def has_limit(stmt: Any) -> bool:
    has_row_limit = getattr(stmt, "_has_row_limiting_clause", None)
    if isinstance(has_row_limit, bool):
        return has_row_limit

    if getattr(stmt, "_limit_clause", None) is not None:
        return True

    if getattr(stmt, "_fetch_clause", None) is not None:
        return True

    getter = getattr(stmt, "get_limit", None)
    if callable(getter):
        if getter() is not None:
            return True

    limit_clause = getattr(stmt, "limit_clause", None)
    if limit_clause is not None:
        return True

    fetch_clause = getattr(stmt, "fetch_clause", None)
    if fetch_clause is not None:
        return True

    return False
