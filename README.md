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

The official [SQLAlchemy](https://www.sqlalchemy.org/) integration for [ParadeDB](https://paradedb.com), including first-class support for managing BM25 indexes with Alembic and running queries using the full ParadeDB API. Follow the [getting started guide](https://docs.paradedb.com/documentation/getting-started/environment#sqlalchemy) to begin.

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
[GitHub Issue](https://github.com/paradedb/sqlalchemy-paradedb/issues/new/choose).

To get community support, you can:

- Post a question in the [ParadeDB Slack Community](https://paradedb.com/slack)
- Ask for help on our [GitHub Discussions](https://github.com/paradedb/paradedb/discussions)

If you need commercial support, please [contact the ParadeDB team](mailto:sales@paradedb.com).

## License

ParadeDB for SQLAlchemy is licensed under the [MIT License](https://github.com/paradedb/sqlalchemy-paradedb?tab=MIT-1-ov-file#readme).
