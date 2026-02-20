from __future__ import annotations

from sqlalchemy import Select
from sqlalchemy.sql.elements import ColumnElement

from . import inspect as pdb_inspect
from . import pdb
from .errors import SnippetWithFuzzyPredicateError


def _assert_snippet_supported(stmt: Select) -> None:
    if pdb_inspect.has_fuzzy_predicate(stmt):
        raise SnippetWithFuzzyPredicateError("Snippets are not supported with fuzzy search predicates")


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
    _assert_snippet_supported(stmt)
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
    _assert_snippet_supported(stmt)
    return stmt.add_columns(
        pdb.snippets(
            field,
            max_num_chars=max_num_chars,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
        ).label(label)
    )


def snippet_positions(
    stmt: Select,
    field: ColumnElement,
    *,
    label: str = "snippet_positions",
) -> Select:
    _assert_snippet_supported(stmt)
    return stmt.add_columns(pdb.snippet_positions(field).label(label))
