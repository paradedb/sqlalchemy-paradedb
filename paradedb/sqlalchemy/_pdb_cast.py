from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import literal
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ClauseElement, ColumnElement


class PDBCast(ColumnElement[Any]):
    """Render an expression cast to a ParadeDB pseudo type, e.g. expr::pdb.boost(2)."""

    inherit_cache = True

    def __init__(self, expr: ClauseElement, type_name: str, args: Sequence[Any] = ()) -> None:
        self.expr = expr
        self.type_name = type_name
        self.args = tuple(args)


def _render_cast_arg(arg: Any, compiler, **kw: Any) -> str:
    if isinstance(arg, ClauseElement):
        return compiler.process(arg, **kw)
    if isinstance(arg, bool):
        return "t" if arg else "f"
    if isinstance(arg, (int, float)):
        return str(arg)
    if isinstance(arg, str):
        return "'" + arg.replace("'", "''") + "'"
    return compiler.process(literal(arg), **kw)


@compiles(PDBCast, "postgresql")
def _compile_pdb_cast(element: PDBCast, compiler, **kw: Any) -> str:
    expr_sql = compiler.process(element.expr, **kw)
    if element.args:
        args_sql = ", ".join(_render_cast_arg(arg, compiler, **kw) for arg in element.args)
        return f"{expr_sql}::pdb.{element.type_name}({args_sql})"
    return f"{expr_sql}::pdb.{element.type_name}()"


@compiles(PDBCast)
def _compile_pdb_cast_default(element: PDBCast, compiler, **kw: Any) -> str:
    raise NotImplementedError("ParadeDB casts are only supported for PostgreSQL dialects")
