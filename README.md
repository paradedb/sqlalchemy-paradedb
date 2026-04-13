<!-- ParadeDB: Postgres for Search and Analytics -->
<h1 align="center">
  <a href="https://paradedb.com"><img src="https://github.com/paradedb/paradedb/raw/main/docs/logo/readme.svg" alt="ParadeDB"></a>
<br>
</h1>

<p align="center">
  <b>Simple, Elastic-quality search for Postgres</b><br/>
</p>

<h3 align="center">
  <a href="https://paradedb.com">Website</a> &bull;
  <a href="https://docs.paradedb.com">Docs</a> &bull;
  <a href="https://paradedb.com/slack/">Community</a> &bull;
  <a href="https://paradedb.com/blog/">Blog</a> &bull;
  <a href="https://docs.paradedb.com/changelog/">Changelog</a>
</h3>

---

# sqlalchemy-paradedb

[![PyPI](https://img.shields.io/pypi/v/sqlalchemy-paradedb)](https://pypi.org/project/sqlalchemy-paradedb/)
[![Python Versions](https://img.shields.io/pypi/pyversions/sqlalchemy-paradedb)](https://pypi.org/project/sqlalchemy-paradedb/)
[![Downloads](https://img.shields.io/pypi/dm/sqlalchemy-paradedb)](https://pypi.org/project/sqlalchemy-paradedb/)
[![Codecov](https://codecov.io/gh/paradedb/sqlalchemy-paradedb/graph/badge.svg)](https://codecov.io/gh/paradedb/sqlalchemy-paradedb)
[![License](https://img.shields.io/github/license/paradedb/sqlalchemy-paradedb?color=blue)](https://github.com/paradedb/sqlalchemy-paradedb?tab=MIT-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fparadedb.com%2Fslack)](https://paradedb.com/slack)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

The official [SQLAlchemy](https://www.sqlalchemy.org/) integration for [ParadeDB](https://paradedb.com) including first class support for for managing BM25 indexes with Alembic and running queries using the full ParadeDB API.

## Usage

Follow the [getting started guide](https://docs.paradedb.com/documentation/getting-started/environment#sqlalchemy) to begin.

## Requirements & Compatibility

| Component  | Supported                     |
| ---------- | ----------------------------- |
| Python     | 3.10+                         |
| SQLAlchemy | 2.0.32+                       |
| ParadeDB   | 0.22.0+                       |
| PostgreSQL | 15+ (with ParadeDB extension) |

## Examples

- [Quick Start](examples/quickstart/quickstart.py)
- [Faceted Search](examples/faceted_search/faceted_search.py)
- [Autocomplete](examples/autocomplete/autocomplete.py)
- [More Like This](examples/more_like_this/more_like_this.py)
- [Hybrid Search (RRF)](examples/hybrid_rrf/hybrid_rrf.py)
- [RAG](examples/rag/rag.py)

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
