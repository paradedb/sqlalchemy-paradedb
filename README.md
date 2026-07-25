<h1 align="center">
  <a href="https://paradedb.com">
    <picture align=center>
      <source media="(prefers-color-scheme: dark)" srcset="https://github.com/paradedb/paradedb/raw/main/docs/logo/paradedb-logo-dark-large.svg">
      <source media="(prefers-color-scheme: light)" srcset="https://github.com/paradedb/paradedb/raw/main/docs/logo/paradedb-logo-light-large.svg">
      <img alt="The ParadeDB logo." src="https://github.com/paradedb/paradedb/raw/main/docs/logo/paradedb-logo-light-large.svg">
    </picture>
  </a>
  <br>
</h1>

<p align="center">
  <b>Search without a second system.</b><br/>
  One Postgres for your application data, full-text search, vector retrieval, and aggregations.
</p>

<h3 align="center">
  <a href="https://paradedb.com">Website</a> &bull;
  <a href="https://docs.paradedb.com">Docs</a> &bull;
  <a href="https://paradedb.com/slack/">Community</a> &bull;
  <a href="https://paradedb.com/blog/">Blog</a> &bull;
  <a href="https://docs.paradedb.com/changelog/">Changelog</a>
</h3>

<p align="center">
  <a href="https://pypi.org/project/sqlalchemy-paradedb/"><img src="https://img.shields.io/pypi/v/sqlalchemy-paradedb" alt="PyPI"></a>&nbsp;
  <a href="https://pypi.org/project/sqlalchemy-paradedb/"><img src="https://img.shields.io/pypi/pyversions/sqlalchemy-paradedb" alt="Python Versions"></a>&nbsp;
  <a href="https://pypi.org/project/sqlalchemy-paradedb/"><img src="https://img.shields.io/pypi/dm/sqlalchemy-paradedb" alt="Downloads"></a>&nbsp;
  <a href="https://codecov.io/gh/paradedb/sqlalchemy-paradedb"><img src="https://codecov.io/gh/paradedb/sqlalchemy-paradedb/graph/badge.svg" alt="Codecov"></a>&nbsp;
  <a href="https://github.com/paradedb/sqlalchemy-paradedb?tab=MIT-1-ov-file#readme"><img src="https://img.shields.io/github/license/paradedb/sqlalchemy-paradedb?color=blue" alt="License"></a>&nbsp;
  <a href="https://paradedb.com/slack"><img src="https://img.shields.io/badge/Join%20Slack-purple?logo=slack" alt="Community"></a>&nbsp;
  <a href="https://x.com/paradedb"><img src="https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb" alt="Follow @paradedb"></a>
</p>

---

## ParadeDB for SQLAlchemy

The official [SQLAlchemy](https://www.sqlalchemy.org/) integration for [ParadeDB](https://paradedb.com) (powered by the [`pg_search`](https://github.com/paradedb/paradedb) Postgres extension), including first-class support for managing ParadeDB indexes with Alembic and running queries using the full ParadeDB API. Follow the [getting started guide](https://docs.paradedb.com/documentation/getting-started/environment#sqlalchemy) to begin.

## Requirements & Compatibility

| Component  | Supported                     |
| ---------- | ----------------------------- |
| Python     | 3.10+                         |
| SQLAlchemy | 2.0.32+                       |
| ParadeDB   | 0.25.0+                       |
| PostgreSQL | 15+ (with ParadeDB extension) |

## Vector Search

pg_search indexes pgvector `vector` columns directly inside ParadeDB indexes, so no pgvector ORM library is needed. Declare a `vector(n)` column with the built-in `Vector` type, add it to the ParadeDB index with `VectorField` and a distance metric, and order by the matching distance function:

```python
from sqlalchemy import Index, select
from paradedb.sqlalchemy import search, vector
from paradedb.sqlalchemy.indexing import ParadeDBField, VectorField
from paradedb.sqlalchemy.vector import Vector


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float]] = mapped_column(Vector(3), nullable=False)


Index(
    "products_bm25_idx",
    ParadeDBField(Product.id),
    ParadeDBField(Product.description),
    VectorField(Product.embedding, metric="l2"),  # metric: "l2" (default), "cosine", or "ip"
    postgresql_using="paradedb",
    postgresql_with={"key_field": "id"},
)

# Top-K query: a @@@ predicate (e.g. search.all) and a LIMIT are required
# for index pushdown. A pure vector query uses the match-all predicate.
stmt = (
    select(Product.id)
    .where(search.all(Product.id))
    .order_by(vector.l2_distance(Product.embedding, [1.0, 0.0, 0.0]))
    .limit(10)
)
```

The ORDER BY distance function must match the index metric: `vector.l2_distance` (`<->`) with `metric="l2"`, `vector.cosine_distance` (`<=>`) with `metric="cosine"`, and `vector.inner_product` (`<#>`) with `metric="ip"`. A mismatched pair still returns correct results but silently loses Top-K index pushdown.

Vector search requires a ParadeDB build with vector-in-index support and the `vector` (pgvector) extension installed.

## Examples

- [Quick Start](examples/quickstart/quickstart.py)
- [Faceted Search](examples/faceted_search/faceted_search.py)
- [Autocomplete](examples/autocomplete/autocomplete.py)
- [More Like This](examples/more_like_this/more_like_this.py)
- [Hybrid Search (RRF)](examples/hybrid_rrf/hybrid_rrf.py)
- [RAG](examples/rag/rag.py)
- [Vector Search](examples/vector_search/vector_search.py)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, running tests, linting, and the PR workflow.

## Support

If you're missing a feature or have found a bug, please open a
[GitHub Issue](https://github.com/paradedb/sqlalchemy-paradedb/issues/new/choose).

To get community support, you can:

- Post a question in the [ParadeDB Slack Community](https://paradedb.com/slack)
- Ask for help on our [GitHub Discussions](https://github.com/paradedb/paradedb/discussions)

If you need commercial support, please [contact the ParadeDB team](mailto:sales@paradedb.com).

## License

ParadeDB for SQLAlchemy is licensed under the [MIT License](https://github.com/paradedb/sqlalchemy-paradedb?tab=MIT-1-ov-file#readme).
