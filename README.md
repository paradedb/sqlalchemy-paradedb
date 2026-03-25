# sqlalchemy-paradedb

[![PyPI](https://img.shields.io/pypi/v/sqlalchemy-paradedb)](https://pypi.org/project/sqlalchemy-paradedb/)
[![Codecov](https://codecov.io/gh/paradedb/sqlalchemy-paradedb/graph/badge.svg)](https://codecov.io/gh/paradedb/sqlalchemy-paradedb)
[![CI](https://github.com/paradedb/sqlalchemy-paradedb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/paradedb/sqlalchemy-paradedb/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/paradedb/sqlalchemy-paradedb?color=blue)](https://github.com/paradedb/sqlalchemy-paradedb?tab=MIT-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fparadedb.com%2Fslack)](https://paradedb.com/slack)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

[ParadeDB](https://paradedb.com) integration for SQLAlchemy: typed helpers for BM25 indexes, search predicates, scoring, snippets, facets, and migration ergonomics.

## Requirements & Compatibility

| Component  | Supported                     |
| ---------- | ----------------------------- |
| Python     | 3.10+                         |
| SQLAlchemy | 2.0.32+                       |
| ParadeDB   | 0.21.0+ (`pg_search`)         |
| PostgreSQL | 17+ (with ParadeDB extension) |

## Installation

```bash
uv add sqlalchemy-paradedb
```

For local development:

```bash
uv sync --extra test --extra dev
```

## Quick Start

### Prerequisites

Install `pg_search` in your Postgres database and connect SQLAlchemy to that database.

### Create a BM25 Index

```python
from sqlalchemy import Index
from paradedb.sqlalchemy import indexing

products_bm25_idx = Index(
    "products_bm25_idx",
    indexing.BM25Field(Product.id),
    indexing.BM25Field(
        Product.description,
        tokenizer=indexing.tokenize.unicode(lowercase=True),
    ),
    indexing.BM25Field(
        Product.category,
        tokenizer=indexing.tokenize.literal(),
    ),
    postgresql_using="bm25",
    postgresql_with={"key_field": "id"},
)
```

For JSON columns named `metadata`, use `metadata_` as the ORM attribute name.

### Query with ParadeDB Predicates

```python
from sqlalchemy import select
from sqlalchemy.orm import Session
from paradedb.sqlalchemy import pdb, search

stmt = (
    select(Product.id, Product.description)
    .where(search.match_any(Product.description, "running", "shoes"))
    .order_by(pdb.score(Product.id).desc())
    .limit(10)
)

with Session(engine) as session:
    rows = session.execute(stmt).all()
```

### Rows + Facets in a Single Query

```python
from sqlalchemy import select
from sqlalchemy.orm import Session
from paradedb.sqlalchemy import facets, search

base = (
    select(Product.id, Product.description)
    .where(search.match_all(Product.description, "running"))
    .order_by(Product.id)
    .limit(10)
)

stmt = facets.with_rows(
    base,
    agg=facets.multi(
        facets.value_count(field="id"),
        facets.terms(field="category", size=10),
    ),
    key_field=Product.id,
)

with Session(engine) as session:
    rows = session.execute(stmt).all()
    facet_payload = facets.extract(rows)
```

## Search Patterns

### Fuzzy Matching

```python
from paradedb.sqlalchemy import search

search.term(Product.description, "shose", distance=1)
search.match_any(Product.description, "wirless", distance=1, prefix=True)
search.term(Product.description, "rnnuing", distance=1, transpose_cost_one=True)
```

Use fuzzy options on `term`, `match_any`, or `match_all`; there is no separate `search.fuzzy(...)` helper.

### Phrase Prefix and More-Like-This

```python
from paradedb.sqlalchemy import search

search.phrase_prefix(Product.description, ["running", "sh"])
search.more_like_this(Product.id, document_id=1, fields=["description"])
```

### Proximity Composition

```python
from sqlalchemy import select
from paradedb.sqlalchemy import search

prox = search.prox_array("running").within(1, search.prox_regex("sho.*"), ordered=True)
stmt = select(Product.id).where(search.proximity(Product.description, prox))
```

## Indexing and Tokenizers

Tokenizer config can be expressed as a structured mapping:

```python
from sqlalchemy import Index
from paradedb.sqlalchemy import indexing

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

Validate that a field is indexed with the expected tokenizer:

```python
from paradedb.sqlalchemy import indexing

indexing.assert_indexed(engine, Product.category, tokenizer="literal")
```

Inspect BM25 metadata for a mapped table:

```python
from paradedb.sqlalchemy import indexing

meta = indexing.describe(engine, Product.__table__)
```

## Alembic Operations

Import once in migration environment startup so Alembic registers ParadeDB operations:

```python
import paradedb.sqlalchemy.alembic  # noqa: F401
```

Use custom operations in migrations:

```python
op.create_bm25_index(
    "products_bm25_idx",
    "products",
    ["id", "description"],
    key_field="id",
    table_schema="public",
)
op.reindex_bm25("products_bm25_idx", concurrently=True, schema="public")
op.drop_bm25_index("products_bm25_idx", if_exists=True, schema="public")
```

`op.reindex_bm25(..., concurrently=True)` must run outside a transaction (autocommit block).

## Diagnostics Helpers

`paradedb.sqlalchemy.diagnostics` exposes wrapper functions for ParadeDB diagnostics:

```python
from paradedb.sqlalchemy import diagnostics

indexes = diagnostics.paradedb_indexes(engine)
segments = diagnostics.paradedb_index_segments(engine, "products_bm25_idx")
check = diagnostics.paradedb_verify_index(engine, "products_bm25_idx", sample_rate=0.1)
all_checks = diagnostics.paradedb_verify_all_indexes(engine, schema_pattern="public")
```

## Common Errors

### `with_rows requires ORDER BY`

```python
from sqlalchemy import select
from paradedb.sqlalchemy import facets

# Missing order_by(...)
base = select(Product.id).limit(10)
facets.with_rows(base, agg=facets.value_count(field="id"), key_field=Product.id)
```

### `with_rows requires LIMIT`

```python
from sqlalchemy import select
from paradedb.sqlalchemy import facets

# Missing limit(...)
base = select(Product.id).order_by(Product.id)
facets.with_rows(base, agg=facets.value_count(field="id"), key_field=Product.id)
```

### `with_rows requires a ParadeDB predicate`

```python
from sqlalchemy import select
from paradedb.sqlalchemy import facets

# ensure_predicate=False disables automatic search.all(...) injection
facets.with_rows(
    select(Product.id).order_by(Product.id).limit(10),
    agg=facets.value_count(field="id"),
    key_field=Product.id,
    ensure_predicate=False,
)
```

### `tokenizer config requires 'tokenizer'`

```python
from paradedb.sqlalchemy import indexing

indexing.tokenize.from_config({"filters": ["lowercase"]})
```

## Examples

- [Quick Start](examples/quickstart/quickstart.py)
- [Faceted Search](examples/faceted_search/faceted_search.py)
- [Autocomplete](examples/autocomplete/autocomplete.py)
- [More Like This](examples/more_like_this/more_like_this.py)
- [Hybrid Search (RRF)](examples/hybrid_rrf/hybrid_rrf.py)
- [RAG](examples/rag/rag.py)

## Documentation

- **Package Documentation**: <https://paradedb.github.io/django-paradedb>
- **ParadeDB Official Docs**: <https://docs.paradedb.com>
- **ParadeDB Website**: <https://paradedb.com>

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, running tests, linting, and the PR workflow.

## Support

If you're missing a feature or have found a bug, please open a
[GitHub Issue](https://github.com/paradedb/django-paradedb/issues/new/choose).

To get community support, you can:

- Post a question in the [ParadeDB Slack Community](https://paradedb.com/slack)
- Ask for help on our [GitHub Discussions](https://github.com/paradedb/paradedb/discussions)

If you need commercial support, please [contact the ParadeDB team](mailto:sales@paradedb.com).

## License

sqlalchemy-paradedb is licensed under the [MIT License](https://github.com/paradedb/sqlalchemy-paradedb?tab=MIT-1-ov-file#readme).
