from __future__ import annotations


class ParadeDBError(Exception):
    """Base class for ParadeDB SQLAlchemy integration errors."""


class InvalidArgumentError(ParadeDBError, ValueError):
    """Raised when a helper receives invalid user arguments."""


class InvalidMoreLikeThisOptionsError(InvalidArgumentError):
    """Raised when more_like_this options are missing/conflicting/out-of-range."""


class ParadeDBValidationError(ParadeDBError, ValueError):
    """Base class for ParadeDB index validation errors."""


class MissingKeyFieldError(ParadeDBValidationError):
    """Raised when a ParadeDB index is missing key_field option."""


class InvalidKeyFieldError(ParadeDBValidationError):
    """Raised when ParadeDB key_field is not part of index fields."""


class DuplicateTokenizerAliasError(ParadeDBValidationError):
    """Raised when tokenizer aliases are duplicated in one ParadeDB index."""


class InvalidParadeDBFieldError(ParadeDBValidationError):
    """Raised when non-ParadeDBField expressions are used in a ParadeDB index."""


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


class FieldNotIndexedError(ParadeDBError):
    """Raised when a column is not covered by any ParadeDB index on its table."""
