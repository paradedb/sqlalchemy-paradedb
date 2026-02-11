from __future__ import annotations

from typing import Any

from sqlalchemy.sql import visitors

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
