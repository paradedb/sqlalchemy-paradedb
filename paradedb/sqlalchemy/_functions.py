from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import literal
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ClauseElement, ColumnElement


class PDBFunctionWithNamedArgs(ColumnElement[Any]):
    """Render pdb.<name>(..., key => value) SQL."""

    inherit_cache = True

    def __init__(
        self,
        name: str,
        positional_args: Sequence[ClauseElement],
        named_args: Sequence[tuple[str, Any]],
    ) -> None:
        self.name = name
        self.positional_args = tuple(positional_args)
        self.named_args = tuple(named_args)


def _render_named_arg_value(value: Any, compiler, **kw: Any) -> str:
    if isinstance(value, ClauseElement):
        return compiler.process(value, **kw)
    return compiler.process(literal(value), **kw)


@compiles(PDBFunctionWithNamedArgs, "postgresql")
def _compile_pdb_function_with_named_args(element: PDBFunctionWithNamedArgs, compiler, **kw: Any) -> str:
    positional_sql = [compiler.process(arg, **kw) for arg in element.positional_args]
    named_sql = [f'{name} => {_render_named_arg_value(value, compiler, **kw)}' for name, value in element.named_args]
    args_sql = ", ".join([*positional_sql, *named_sql])
    return f"pdb.{element.name}({args_sql})"


@compiles(PDBFunctionWithNamedArgs)
def _compile_pdb_function_with_named_args_default(element: PDBFunctionWithNamedArgs, compiler, **kw: Any) -> str:
    raise NotImplementedError("ParadeDB function wrappers are only supported for PostgreSQL dialects")
