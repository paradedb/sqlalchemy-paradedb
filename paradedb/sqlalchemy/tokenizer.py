from __future__ import annotations
from dataclasses import dataclass
import re
from typing import Any, Mapping

from paradedb.sqlalchemy.errors import InvalidArgumentError

_VALID_TOKENIZER_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Tokenizer:
    name: str | None = None
    positional_args: tuple[Any, ...] | None = None
    options: Mapping[str, Any] | None = None
    raw_sql: str | None = None

    def render(self) -> str:
        if self.raw_sql is not None:
            return self.raw_sql

        if self.name is None:
            raise InvalidArgumentError("tokenizer name is required unless raw_sql is provided")

        if not self.positional_args and not self.options:
            return f"pdb.{self.name}"

        args = []
        if self.positional_args is not None:
            args = [_render_sql_arg(value) for value in self.positional_args]
        if self.options:
            args.extend([_quote_term(f"{key}={_render_config_value(value)}") for key, value in self.options.items()])
        return f"pdb.{self.name}({','.join(args)})"

    def extract_alias(self) -> str | None:
        if self.options is None:
            return None
        for key, val in self.options.items():
            if key == "alias":
                return val
        return None


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


def unicode_words(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("unicode_words", options=options)


def simple(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("simple", options=options)


def whitespace(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("whitespace", options=options)


def icu(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("icu", options=options)


def chinese_compatible(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("chinese_compatible", options=options)


def jieba(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("jieba", options=options)


def literal(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("literal", options=options)


def literal_normalized(*, options: Mapping[str, Any] | None = None) -> Tokenizer:
    return Tokenizer("literal_normalized", options=options)


def ngram(
    min_gram: int,
    max_gram: int,
    *,
    options: Mapping[str, Any] | None = None,
) -> Tokenizer:
    return Tokenizer("ngram", positional_args=(min_gram, max_gram), options=options)


def edge_ngram(
    min_gram: int,
    max_gram: int,
    *,
    options: Mapping[str, Any] | None = None,
) -> Tokenizer:
    return Tokenizer("edge_ngram", positional_args=(min_gram, max_gram), options=options)


def lindera(
    dictionary: str,
    *,
    options: Mapping[str, Any] | None = None,
) -> Tokenizer:
    return Tokenizer("lindera", positional_args=(dictionary,), options=options)


def regex_pattern(
    pattern: str,
    *,
    options: Mapping[str, Any] | None = None,
) -> Tokenizer:
    return Tokenizer("regex_pattern", positional_args=(pattern,), options=options)


def source_code(
    *,
    options: Mapping[str, Any] | None = None,
) -> Tokenizer:
    return Tokenizer("source_code", options=options)


def raw(sql: str) -> Tokenizer:
    return Tokenizer(raw_sql=sql)
