from __future__ import annotations


def value_count(*, field: str) -> dict[str, dict[str, str]]:
    return {"value_count": {"field": field}}


def avg(*, field: str) -> dict[str, dict[str, str]]:
    return {"avg": {"field": field}}


def terms(*, field: str, size: int | None = None) -> dict[str, dict[str, object]]:
    payload: dict[str, object] = {"field": field}
    if size is not None:
        payload["size"] = size
    return {"terms": payload}


def multi(*aggs: dict[str, object]) -> dict[str, object]:
    merged: dict[str, object] = {}
    for agg in aggs:
        merged.update(agg)
    return merged
