from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Select
from sqlalchemy.sql.elements import ColumnElement

from . import inspect as pdb_inspect
from . import pdb, search
from ._select_introspection import has_limit, has_order_by
from .errors import (
    FacetRequiresLimitError,
    FacetRequiresOrderByError,
    FacetRequiresParadeDBPredicateError,
)
from .validation import require_non_empty_sequence


def _field_spec(name: str, field: str) -> dict[str, dict[str, str]]:
    return {name: {"field": field}}


def value_count(*, field: str) -> dict[str, dict[str, str]]:
    return _field_spec("value_count", field)


def avg(*, field: str) -> dict[str, dict[str, str]]:
    return _field_spec("avg", field)


def sum(*, field: str) -> dict[str, dict[str, str]]:
    return _field_spec("sum", field)


def min(*, field: str) -> dict[str, dict[str, str]]:
    return _field_spec("min", field)


def max(*, field: str) -> dict[str, dict[str, str]]:
    return _field_spec("max", field)


def stats(*, field: str) -> dict[str, dict[str, str]]:
    return _field_spec("stats", field)


def percentiles(*, field: str, percents: list[float | int]) -> dict[str, dict[str, object]]:
    require_non_empty_sequence(percents, field_name="percents")
    return {"percentiles": {"field": field, "percents": list(percents)}}


def terms(*, field: str, size: int | None = None) -> dict[str, dict[str, object]]:
    payload: dict[str, object] = {"field": field}
    if size is not None:
        payload["size"] = size
    return {"terms": payload}


def histogram(*, field: str, interval: int | float) -> dict[str, dict[str, object]]:
    return {"histogram": {"field": field, "interval": interval}}


def date_histogram(*, field: str, fixed_interval: str) -> dict[str, dict[str, str]]:
    return {"date_histogram": {"field": field, "fixed_interval": fixed_interval}}


def range(*, field: str, ranges: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {"range": {"field": field, "ranges": ranges}}


def top_hits(
    *,
    size: int | None = None,
    from_: int | None = None,
    sort: list[dict[str, Any]] | None = None,
    docvalue_fields: list[str] | None = None,
) -> dict[str, dict[str, object]]:
    payload: dict[str, object] = {}
    if size is not None:
        payload["size"] = size
    if from_ is not None:
        payload["from"] = from_
    if sort is not None:
        payload["sort"] = sort
    if docvalue_fields is not None:
        payload["docvalue_fields"] = docvalue_fields
    return {"top_hits": payload}


def multi(*aggs: dict[str, object]) -> dict[str, object]:
    merged: dict[str, object] = {}
    for agg in aggs:
        merged.update(agg)
    return merged


def ensure_operator(stmt: Select, *, key_field: ColumnElement) -> Select:
    if pdb_inspect.has_paradedb_predicate(stmt):
        return stmt
    return stmt.where(search.all(key_field))


@dataclass(frozen=True)
class FacetPlan:
    label: str = "facets"

    def extract(self, rows: list[object]) -> Any | None:
        if not rows:
            return None
        first = rows[0]
        mapping = getattr(first, "_mapping", None)
        if mapping is not None and self.label in mapping:
            return mapping[self.label]
        if isinstance(first, Sequence) and not isinstance(first, (str, bytes)):
            return first[-1]
        return None


def with_rows(
    base_stmt: Select,
    *,
    agg: dict[str, Any],
    key_field: ColumnElement,
    label: str = "facets",
    ensure_predicate: bool = True,
) -> tuple[Select, FacetPlan]:
    if not has_order_by(base_stmt):
        raise FacetRequiresOrderByError("with_rows requires ORDER BY")
    if not has_limit(base_stmt):
        raise FacetRequiresLimitError("with_rows requires LIMIT")

    stmt = ensure_operator(base_stmt, key_field=key_field) if ensure_predicate else base_stmt
    if not ensure_predicate and not pdb_inspect.has_paradedb_predicate(stmt):
        raise FacetRequiresParadeDBPredicateError("with_rows requires a ParadeDB predicate")

    stmt = stmt.add_columns(pdb.agg(agg).over().label(label))
    return stmt, FacetPlan(label=label)
