from __future__ import annotations

from collections.abc import Sequence
from .errors import InvalidArgumentError

def require_non_empty_strings(
    values: Sequence[str],
    *,
    field_name: str,
    error_cls: type[InvalidArgumentError] = InvalidArgumentError,
) -> None:
    if any((not isinstance(value, str)) or (not value.strip()) for value in values):
        raise error_cls(f"{field_name} entries must be non-empty strings")


def require_non_empty_sequence(
    values: Sequence[object],
    *,
    field_name: str,
    error_cls: type[InvalidArgumentError] = InvalidArgumentError,
) -> None:
    if not values:
        raise error_cls(f"{field_name} must contain at least one value")


def require_non_negative(
    value: int | float,
    *,
    field_name: str,
    error_cls: type[InvalidArgumentError] = InvalidArgumentError,
) -> None:
    if value < 0:
        raise error_cls(f"{field_name} must be >= 0")


def require_positive(
    value: int | float,
    *,
    field_name: str,
    error_cls: type[InvalidArgumentError] = InvalidArgumentError,
) -> None:
    if value <= 0:
        raise error_cls(f"{field_name} must be > 0")


def require_ordered_bounds(
    lower: int | float,
    upper: int | float,
    *,
    lower_name: str,
    upper_name: str,
    error_cls: type[InvalidArgumentError] = InvalidArgumentError,
) -> None:
    if lower > upper:
        raise error_cls(f"{lower_name} cannot be greater than {upper_name}")
