# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.0] - 2026-08-04

### Added

- Support for the vector index build options `centroid_ratio`, `training_samples_per_centroid`, and `cluster_replication` (pg_search 0.25.0+). Construct a validated `paradedb.VectorIndexOptions` and unpack it into `postgresql_with` alongside `key_field` (raw dict entries also work), and in Alembic pass either form to the new `with_options` parameter of `op.create_paradedb_index`/`op.drop_paradedb_index`. Autogenerate emits and round-trips the options, and reflection compares real-valued options tolerantly so existing indexes don't churn.

## [0.9.0] - 2026-08-04

### Added

- Native vector search support: a `Vector(n)` column type, `VectorField` for declaring pgvector columns with a distance metric (`l2`, `cosine`, `ip`) inside ParadeDB indexes, and `vector.l2_distance` / `vector.cosine_distance` / `vector.inner_product` query expressions for Top-K ordering. Alembic autogenerate round-trips ParadeDB indexes containing vector opclasses without churn.

### Changed

- **Breaking:** the bm25-named API has been renamed to paradedb, and indexes always use the `paradedb` index access method (requires pg_search 0.25.0+). `BM25Field` is now `ParadeDBField`, `validate_bm25_index` is now `validate_paradedb_index`, the Alembic operations `op.create_bm25_index`/`op.drop_bm25_index`/`op.reindex_bm25` are now `op.create_paradedb_index`/`op.drop_paradedb_index`/`op.reindex_paradedb` (with operation classes `CreateParadeDBIndexOp`/`DropParadeDBIndexOp`/`ReindexParadeDBOp`), and the errors `BM25ValidationError`/`InvalidBM25FieldError` are now `ParadeDBValidationError`/`InvalidParadeDBFieldError`. `Index(postgresql_using="paradedb")` is the only recognized access method; index DDL always emits `USING paradedb`. Existing migrations that call the bm25-named operations must be updated to the paradedb names.

## [0.8.0] - 2026-07-14

### Changed

- Documentation and copy.

## [0.7.0] - 2026-06-12

### Removed

- **BREAKING**: The `document_ids` parameter from `more_like_this` was removed. To replicate the same behavior, combine multiple single-id more like this queries with `or`.

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

[0.9.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.9.0
[0.8.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.8.0
[0.7.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.7.0
[0.6.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.6.0
[0.5.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.5.0
[0.4.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.4.0
[0.3.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.3.0
[0.2.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.2.0
[0.1.0]: https://github.com/paradedb/sqlalchemy-paradedb/releases/tag/v0.1.0
