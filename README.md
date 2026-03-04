# sqlalchemy-paradedb

Typed SQLAlchemy helpers for ParadeDB BM25 indexing and query composition.

## Requirements

- Python 3.10+
- PostgreSQL with ParadeDB (`pg_search`) available
- SQLAlchemy 2.x

## Install

```bash
uv add sqlalchemy-paradedb
```

For local development:

```bash
uv sync --extra test --extra dev
```

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

For JSON columns named `metadata`, use `metadata_` as the ORM attribute name.

Tokenizer config can be expressed as structured dicts:

```python
products_bm25_idx = Index(
    "products_bm25_idx",
    indexing.BM25Field(Product.id),
    indexing.BM25Field(
        Product.description,
        tokenizer=indexing.tokenize.from_config(
            {
                "tokenizer": "simple",
                "filters": ["lowercase", "stemmer"],
                "stemmer": "english",
                "alias": "description_simple",
            }
        ),
    ),
    indexing.BM25Field(
        Product.description,
        tokenizer=indexing.tokenize.from_config(
            {
                "tokenizer": "ngram",
                "args": [3, 8],
                "named_args": {"prefix_only": True},
                "alias": "description_ngram",
            }
        ),
    ),
    postgresql_using="bm25",
    postgresql_with={"key_field": "id"},
)
```

## Query Examples

Fuzzy search uses the normal term-style builders:

```python
search.term(Product.description, "shose", distance=1)
search.match_any(Product.description, "wirless", distance=1, prefix=True)
search.term(Product.description, "rnnuing", distance=1, transpose_cost_one=True)
```

There is no separate `search.fuzzy(...)` helper. Use the standard term-style
builders with fuzzy arguments instead.

Proximity helpers can be composed before you apply them:

```python
prox = search.prox_array("running").near(search.prox_regex("sho.*"), distance=1, ordered=True)
stmt = select(Product.id).where(search.proximity(Product.description, prox))
```

Rows + facets:

```python
from sqlalchemy import select
from paradedb.sqlalchemy import facets, pdb, search

base = (
    select(
        Product.id,
        Product.description,
    )
    .where(search.match_all(Product.description, "running"))
    .order_by(Product.id)
    .limit(10)
)
stmt, facet_plan = facets.with_rows(base, agg=facets.value_count(field="id"), key_field=Product.id)
```

## Alembic Operations

Import once in migration env startup for its registration side effects, so
Alembic knows about the custom BM25 operations:

```python
import paradedb.sqlalchemy.alembic  # noqa: F401
```

```python
op.create_bm25_index("products_bm25_idx", "products", ["id", "description"], key_field="id")
op.reindex_bm25("products_bm25_idx", concurrently=True)
op.drop_bm25_index("products_bm25_idx", if_exists=True)
```

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
uv run --extra test pytest tests/unit
```

Integration tests (requires running ParadeDB):

```bash
PARADEDB_TEST_DSN=postgresql+psycopg://postgres:postgres@localhost:5443/postgres uv run --extra test pytest -m integration
```
