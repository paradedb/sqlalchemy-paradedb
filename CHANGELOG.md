# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.6.0] - 2026-04-21

### Changed

- **BREAKING**: Streamlined the index creation tokenizer API and introduced it to the query interface.

## [0.5.0] - 2026-04-16

### Added

- Added support for passing tokenizer params in query functions.

## [0.4.0] - 2026-04-13

### Added

- Added support for passing functions into queries such as `search.term(Table1.field1, func.trim(keyword))`.

## [0.3.0] - 2026-04-07

### Fixed

- Fixed support for auto-generating migrations using Alembic.

## [0.2.0] - 2026-04-01

### Added

- The ability to pass `StrEnum`s as parameters into queries.

## [0.1.0] - 2026-03-25

### Added

- Full BM25 search/query helper set with advanced operators.
- Facet and aggregation builders plus rows+facets helper.
- Alembic custom operations and autogenerate render hooks.
- Centralized validation helpers and expanded runtime guard errors.
- Unit/integration suites for indexing, querying, facets, and migrations.
- CI workflow for lint, typing, unit, and integration checks.
- Example scripts for quickstart, facets, autocomplete, MLT, hybrid RRF, and RAG retrieval.

[0.6.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.6.0
[0.5.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.5.0
[0.4.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.4.0
[0.3.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.3.0
[0.2.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.2.0
[0.1.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.1.0
