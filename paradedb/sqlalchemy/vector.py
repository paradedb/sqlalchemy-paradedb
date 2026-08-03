from __future__ import annotations

from typing import Any

from sqlalchemy import literal
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import ClauseElement, ColumnElement
from sqlalchemy.types import UserDefinedType

from .errors import InvalidArgumentError
from .validation import require_positive

VECTOR_OPCLASSES: dict[str, str] = {
    "l2": "vector_l2_ops",
    "cosine": "vector_cosine_ops",
    "ip": "vector_ip_ops",
}

_L2_DISTANCE: Any = operators.custom_op("<->", precedence=5)
_COSINE_DISTANCE: Any = operators.custom_op("<=>", precedence=5)
_INNER_PRODUCT: Any = operators.custom_op("<#>", precedence=5)


def _serialize_vector(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return "[" + ",".join(str(float(component)) for component in value) + "]"
    except TypeError as exc:
        raise InvalidArgumentError("vector value must be a sequence of numbers or a vector literal string") from exc


def _deserialize_vector(value: Any) -> list[float]:
    if isinstance(value, (list, tuple)):
        return [float(component) for component in value]
    text = bytes(value).decode() if isinstance(value, (bytes, memoryview)) else str(value)
    inner = text.strip().removeprefix("[").removesuffix("]").strip()
    if not inner:
        return []
    return [float(part) for part in inner.split(",")]


class Vector(UserDefinedType):
    """The pgvector ``vector(n)`` column type, mapping Python float sequences to vector literals."""

    cache_ok = True

    def __init__(self, dim: int | None = None) -> None:
        if dim is not None:
            require_positive(dim, field_name="dim")
        self.dim = dim

    def get_col_spec(self, **kw: Any) -> str:
        if self.dim is None:
            return "vector"
        return f"vector({self.dim})"

    def bind_processor(self, dialect):
        def process(value: Any) -> str | None:
            if value is None:
                return None
            return _serialize_vector(value)

        return process

    def literal_processor(self, dialect):
        def process(value: Any) -> str:
            return "'" + _serialize_vector(value).replace("'", "''") + "'"

        return process

    def result_processor(self, dialect, coltype):
        def process(value: Any) -> list[float] | None:
            if value is None:
                return None
            return _deserialize_vector(value)

        return process


ischema_names["vector"] = Vector


def _to_vector_operand(value: Any) -> ClauseElement:
    if isinstance(value, ClauseElement):
        return value
    return literal(value, type_=Vector())


def l2_distance(field: ColumnElement, value: Any) -> ColumnElement:
    """``field <-> value``. Pair with an index opclass of ``vector_l2_ops`` (metric ``"l2"``)."""
    return field.operate(_L2_DISTANCE, _to_vector_operand(value))


def cosine_distance(field: ColumnElement, value: Any) -> ColumnElement:
    """``field <=> value``. Pair with an index opclass of ``vector_cosine_ops`` (metric ``"cosine"``)."""
    return field.operate(_COSINE_DISTANCE, _to_vector_operand(value))


def inner_product(field: ColumnElement, value: Any) -> ColumnElement:
    """``field <#> value``. Pair with an index opclass of ``vector_ip_ops`` (metric ``"ip"``)."""
    return field.operate(_INNER_PRODUCT, _to_vector_operand(value))
