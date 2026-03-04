from __future__ import annotations

from typing import Any

from sqlalchemy.sql import visitors

from ._pdb_cast import PDBCast

_PARADEDB_PREDICATE_OPS = {"|||", "&&&", "###", "@@@", "==="}


def collect_paradedb_operators(clause: Any) -> set[str]:
    operators: set[str] = set()

    def visit_binary(binary) -> None:
        opstring = getattr(binary.operator, "opstring", None)
        if opstring in _PARADEDB_PREDICATE_OPS:
            operators.add(opstring)

    visitors.traverse(clause, {}, {"binary": visit_binary})
    return operators


def has_paradedb_predicate(clause: Any) -> bool:
    return bool(collect_paradedb_operators(clause))


def _contains_fuzzy_cast(expr: Any) -> bool:
    if isinstance(expr, PDBCast):
        if expr.type_name == "fuzzy":
            return True
        return _contains_fuzzy_cast(expr.expr)
    return False


def has_fuzzy_predicate(clause: Any) -> bool:
    found = False

    def visit_binary(binary) -> None:
        nonlocal found
        if found:
            return
        opstring = getattr(binary.operator, "opstring", None)
        if opstring not in _PARADEDB_PREDICATE_OPS:
            return
        right = getattr(binary, "right", None)
        if _contains_fuzzy_cast(right):
            found = True

    visitors.traverse(clause, {}, {"binary": visit_binary})
    return found
