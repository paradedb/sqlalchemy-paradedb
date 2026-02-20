# sqlalchemy-paradedb

Typed SQLAlchemy helpers for ParadeDB BM25 indexing and query composition.

## Requirements

- Python 3.10+
- PostgreSQL with ParadeDB (`pg_search`) available
- SQLAlchemy 2.x

## Install

```bash
pip install sqlalchemy-paradedb
```

For local development:

```bash
pip install -e .[test,dev]
```

## Core Modules

- `paradedb.sqlalchemy.indexing`: BM25 field definitions and tokenizer specs.
- `paradedb.sqlalchemy.search`: ParadeDB predicates (`match_all`, `fuzzy`, `parse`, `more_like_this`, etc.).
- `paradedb.sqlalchemy.pdb`: function wrappers (`score`, `snippet`, `snippets`, `agg`).
- `paradedb.sqlalchemy.facets`: aggregate/facet JSON builders and rows+facets helper.
- `paradedb.sqlalchemy.select_with`: select decorators for score/snippet/snippet_positions columns.
- `paradedb.sqlalchemy.alembic`: Alembic operations for BM25 index lifecycle.

## Quickstart

```python
from sqlalchemy import Index, select
from paradedb.sqlalchemy import indexing, search

products_bm25_idx = Index(
    "products_bm25_idx",
    indexing.BM25Field(Product.id),
    indexing.BM25Field(Product.description, tokenizer=indexing.tokenize.unicode(lowercase=True)),
    indexing.BM25Field(Product.category, tokenizer=indexing.tokenize.literal()),
    postgresql_using="bm25",
    postgresql_with={"key_field": "id"},
)

products_bm25_idx.create(engine)

stmt = select(Product.id, Product.description).where(search.match_any(Product.description, "running", "shoes"))
```

Index JSON keys using BM25Field expressions:

```python
from paradedb.sqlalchemy import expr

products_bm25_idx = Index(
    "products_bm25_idx",
    indexing.BM25Field(Product.id),
    indexing.BM25Field(
        expr.json_text(Product.metadata, "color"),
        tokenizer=indexing.tokenize.literal(alias="metadata_color"),
    ),
    indexing.BM25Field(
        expr.json_text(Product.metadata, "location"),
        tokenizer=indexing.tokenize.literal(alias="metadata_location"),
    ),
    postgresql_using="bm25",
    postgresql_with={"key_field": "id"},
)
```

## Query APIs

- Basic predicates: `match_all`, `match_any`, `term`, `phrase`, `fuzzy`, `regex`, `all`
- Advanced predicates: `parse`, `phrase_prefix`, `regex_phrase`, `near`, `proximity`, `more_like_this`
- Scoring/snippets: `pdb.score`, `pdb.snippet`, `pdb.snippets`, `pdb.snippet_positions`, `select_with.score`, `select_with.snippet`, `select_with.snippet_positions`
- Aggregations/facets: `facets.*` builders + `pdb.agg(...)`
- Rows + facets: `facets.with_rows(...)`

## Facets

```python
from sqlalchemy import select
from paradedb.sqlalchemy import facets, pdb, search

stmt = (
    select(
        pdb.agg(facets.value_count(field="id")).label("count"),
        pdb.agg(facets.avg(field="rating")).label("avg_rating"),
        pdb.agg(facets.percentiles(field="rating", percents=[50, 95])).label("rating_percentiles"),
        pdb.agg(facets.top_hits(size=2, sort=[{"rating": "desc"}], docvalue_fields=["id", "rating"])).label("top_hits"),
    )
    .select_from(Product)
    .where(search.match_all(Product.description, "running"))
)
```

## Alembic Operations

Import once in migration env startup so operations are registered:

```python
import paradedb.sqlalchemy.alembic  # noqa: F401
```

Usage:

```python
op.create_bm25_index("products_bm25_idx", "products", ["id", "description"], key_field="id")
op.reindex_bm25("products_bm25_idx", concurrently=True)
op.drop_bm25_index("products_bm25_idx", if_exists=True)
```

## Validation and Guardrails

- Search and facet builders validate option bounds and shapes at build time.
- `select_with.snippet*` raises `SnippetWithFuzzyPredicateError` with fuzzy predicates.
- `facets.with_rows` enforces `ORDER BY` + `LIMIT`, and can auto-inject a ParadeDB sentinel (`pdb.all()`).

## Examples

See `examples/`:

- `quickstart.py`
- `faceted_search.py`
- `autocomplete.py`
- `more_like_this.py`
- `hybrid_rrf.py`
- `rag.py`

## Testing

Unit tests:

```bash
python -m pytest tests/unit
```

Integration tests (requires running ParadeDB):

```bash
PARADEDB_TEST_DSN=postgres://postgres:postgres@localhost:5443/postgres python -m pytest -m integration
```

## CI

GitHub Actions workflow at `.github/workflows/ci.yml` runs:

- Ruff lint
- Mypy type check
- Unit tests
- Integration tests against a ParadeDB service container
