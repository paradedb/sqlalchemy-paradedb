from __future__ import annotations


class ParadeDBError(Exception):
    """Base class for ParadeDB SQLAlchemy integration errors."""


class InvalidArgumentError(ParadeDBError, ValueError):
    """Raised when a helper receives invalid user arguments."""


class InvalidMoreLikeThisOptionsError(InvalidArgumentError):
    """Raised when more_like_this options are missing/conflicting/out-of-range."""


class BM25ValidationError(ParadeDBError, ValueError):
    """Base class for BM25 index validation errors."""


class MissingKeyFieldError(BM25ValidationError):
    """Raised when a BM25 index is missing key_field option."""


class InvalidKeyFieldError(BM25ValidationError):
    """Raised when BM25 key_field is not part of index fields."""


class DuplicateTokenizerAliasError(BM25ValidationError):
    """Raised when tokenizer aliases are duplicated in one BM25 index."""


class InvalidBM25FieldError(BM25ValidationError):
    """Raised when non-BM25Field expressions are used in a BM25 index."""


class RuntimeGuardError(ParadeDBError, ValueError):
    """Base class for runtime guardrail violations on statement builders."""


class SnippetWithFuzzyPredicateError(RuntimeGuardError):
    """Raised when snippet/snippets helpers are used with fuzzy predicates."""


class FacetRuntimeError(RuntimeGuardError):
    """Base class for facet runtime guardrail violations."""


class FacetRequiresOrderByError(FacetRuntimeError):
    """Raised when rows+facets helper is missing ORDER BY."""


class FacetRequiresLimitError(FacetRuntimeError):
    """Raised when rows+facets helper is missing LIMIT."""


class FacetRequiresParadeDBPredicateError(FacetRuntimeError):
    """Raised when rows+facets helper is used without ParadeDB predicate/sentinel."""
