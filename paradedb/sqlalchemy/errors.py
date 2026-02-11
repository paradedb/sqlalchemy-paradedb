from __future__ import annotations


class ParadeDBError(Exception):
    """Base class for ParadeDB SQLAlchemy integration errors."""


class InvalidArgumentError(ParadeDBError, ValueError):
    """Raised when a helper receives invalid user arguments."""


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
