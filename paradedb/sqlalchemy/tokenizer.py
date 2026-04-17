from __future__ import annotations
from dataclasses import dataclass
import re
from typing import Any, Mapping, Sequence

from paradedb.sqlalchemy.errors import InvalidArgumentError

_VALID_TOKENIZER_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TokenizerSpec:
    name: str | None = None
    positional_args: tuple[Any, ...] = ()
    named_args: tuple[tuple[str, Any], ...] = ()
    raw_sql: str | None = None
    alias: str | None = None

    def render(self) -> str:
        if self.raw_sql is not None:
            return self.raw_sql

        if self.name is None:
            raise InvalidArgumentError("tokenizer name is required unless raw_sql is provided")

        if not self.positional_args and not self.named_args:
            return f"pdb.{self.name}"

        args_sql = [_render_sql_arg(value) for value in self.positional_args]
        if self.named_args:
            rendered_options = ",".join(f"{key}={_render_config_value(value)}" for key, value in self.named_args)
            args_sql.append(_quote_term(rendered_options))
        return f"pdb.{self.name}({','.join(args_sql)})"


def _quote_term(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _render_sql_arg(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, str):
        return _quote_term(value)
    raise InvalidArgumentError(f"Unsupported tokenizer arg type: {type(value).__name__}")


def _render_config_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    raise InvalidArgumentError(f"Unsupported tokenizer named arg type: {type(value).__name__}")


def _build_spec(
    name: str,
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
    **kwargs: Any,
) -> TokenizerSpec:
    if not _VALID_TOKENIZER_NAME_RE.match(name):
        raise InvalidArgumentError(
            "tokenizer name must be a bare identifier (letters, digits, underscore); pass arguments via args/named_args"
        )
    if args is not None and isinstance(args, str | bytes):
        raise InvalidArgumentError("tokenizer args must be a sequence, not a string")
    if named_args is not None and not isinstance(named_args, Mapping):
        raise InvalidArgumentError("tokenizer named_args must be a mapping")
    if filters is not None:
        if isinstance(filters, str | bytes):
            raise InvalidArgumentError("tokenizer filters must be a sequence, not a string")
        if not isinstance(filters, Sequence):
            raise InvalidArgumentError("tokenizer filters must be a sequence")

    positional = tuple(args or ())
    normalized: dict[str, Any] = {}

    if alias is not None:
        normalized["alias"] = alias

    if named_args is not None:
        for key, value in named_args.items():
            if value is not None:
                normalized[str(key)] = value

    for key, value in kwargs.items():
        if value is not None:
            normalized[str(key)] = value

    if filters is not None:
        for filter_name in filters:
            key = str(filter_name)
            if key == "stemmer" and stemmer is not None:
                normalized.setdefault("stemmer", stemmer)
            else:
                normalized.setdefault(key, True)
    elif stemmer is not None:
        normalized.setdefault("stemmer", stemmer)

    return TokenizerSpec(
        name=name,
        positional_args=positional,
        named_args=tuple(normalized.items()),
        alias=alias,
    )


def unicode(*, alias: str | None = None, lowercase: bool | None = None, stemmer: str | None = None) -> TokenizerSpec:
    # ParadeDB currently exposes this tokenizer as `unicode_words`.
    return _build_spec(
        "unicode_words",
        alias=alias,
        named_args={"lowercase": lowercase, "stemmer": stemmer},
    )


def simple(
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    return _build_spec(
        "simple",
        alias=alias,
        args=args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def whitespace(
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    return _build_spec(
        "whitespace",
        alias=alias,
        args=args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def icu(
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    return _build_spec(
        "icu",
        alias=alias,
        args=args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def chinese_compatible(
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    return _build_spec(
        "chinese_compatible",
        alias=alias,
        args=args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def jieba(
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    return _build_spec(
        "jieba",
        alias=alias,
        args=args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def literal(*, alias: str | None = None) -> TokenizerSpec:
    return _build_spec("literal", alias=alias)


def literal_normalized(*, alias: str | None = None) -> TokenizerSpec:
    return _build_spec("literal_normalized", alias=alias)


def ngram(
    *,
    alias: str | None = None,
    min_gram: int | None = None,
    max_gram: int | None = None,
    prefix_only: bool | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    positional_args: list[Any] = list(args or ())
    use_positional_bounds = min_gram is not None and max_gram is not None and not positional_args
    if use_positional_bounds:
        positional_args.extend([min_gram, max_gram])

    all_named_args: dict[str, Any] = {}
    if named_args is not None:
        all_named_args.update({str(key): value for key, value in named_args.items()})
    if min_gram is not None and not use_positional_bounds:
        all_named_args["min_gram"] = min_gram
    if max_gram is not None and not use_positional_bounds:
        all_named_args["max_gram"] = max_gram
    if prefix_only is not None:
        all_named_args["prefix_only"] = prefix_only

    return _build_spec(
        "ngram",
        alias=alias,
        args=positional_args,
        named_args=all_named_args,
        filters=filters,
        stemmer=stemmer,
    )


def lindera(
    dictionary: str | None = None,
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    positional_args: list[Any] = list(args or ())
    if dictionary is not None and not positional_args:
        positional_args.append(dictionary)
    return _build_spec(
        "lindera",
        alias=alias,
        args=positional_args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def regex_pattern(
    pattern: str | None = None,
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    positional_args: list[Any] = list(args or ())
    if pattern is not None and not positional_args:
        positional_args.append(pattern)
    return _build_spec(
        "regex_pattern",
        alias=alias,
        args=positional_args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def source_code(
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    return _build_spec(
        "source_code",
        alias=alias,
        args=args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def raw(sql: str, *, alias: str | None = None) -> TokenizerSpec:
    return TokenizerSpec(raw_sql=sql, alias=alias)


def custom(
    tokenizer: str,
    *,
    alias: str | None = None,
    args: Sequence[Any] | None = None,
    named_args: Mapping[str, Any] | None = None,
    filters: Sequence[str] | None = None,
    stemmer: str | None = None,
) -> TokenizerSpec:
    return _build_spec(
        tokenizer,
        alias=alias,
        args=args,
        named_args=named_args,
        filters=filters,
        stemmer=stemmer,
    )


def from_config(config: Mapping[str, Any]) -> TokenizerSpec:
    if not isinstance(config, Mapping):
        raise InvalidArgumentError("tokenizer config must be a mapping")

    allowed_keys = {"tokenizer", "args", "named_args", "filters", "stemmer", "alias"}
    unknown = set(config.keys()) - allowed_keys
    if unknown:
        unknown_csv = ", ".join(sorted(str(key) for key in unknown))
        raise InvalidArgumentError(f"Unknown tokenizer config keys: {unknown_csv}")

    tokenizer = config.get("tokenizer")
    if tokenizer is None:
        raise InvalidArgumentError("tokenizer config requires 'tokenizer'")
    if not isinstance(tokenizer, str):
        raise InvalidArgumentError("tokenizer config 'tokenizer' must be a string")

    return custom(
        tokenizer,
        args=config.get("args"),
        named_args=config.get("named_args"),
        filters=config.get("filters"),
        stemmer=config.get("stemmer"),
        alias=config.get("alias"),
    )
