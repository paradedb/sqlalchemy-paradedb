from __future__ import annotations

from sqlalchemy import Select
from sqlalchemy.sql.elements import ColumnElement

from . import pdb


def score(stmt: Select, field: ColumnElement, *, label: str = "score") -> Select:
    return stmt.add_columns(pdb.score(field).label(label))


def snippet(
    stmt: Select,
    field: ColumnElement,
    *,
    label: str = "snippet",
    start_tag: str | None = None,
    end_tag: str | None = None,
    max_num_chars: int | None = None,
) -> Select:
    return stmt.add_columns(
        pdb.snippet(
            field,
            start_tag=start_tag,
            end_tag=end_tag,
            max_num_chars=max_num_chars,
        ).label(label)
    )


def snippets(
    stmt: Select,
    field: ColumnElement,
    *,
    label: str = "snippets",
    max_num_chars: int | None = None,
    limit: int | None = None,
    offset: int | None = None,
    sort_by: str | None = None,
) -> Select:
    return stmt.add_columns(
        pdb.snippets(
            field,
            max_num_chars=max_num_chars,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
        ).label(label)
    )
