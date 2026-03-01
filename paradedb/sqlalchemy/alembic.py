from __future__ import annotations

import re

from alembic.autogenerate import comparators, renderers
from alembic.operations import Operations
from alembic.operations.ops import MigrateOperation
from alembic.util import DispatchPriority, PriorityDispatchResult
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import ClauseElement


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_qualified(schema: str | None, name: str) -> str:
    if schema:
        return f"{_quote_ident(schema)}.{_quote_ident(name)}"
    return _quote_ident(name)


@Operations.register_operation("create_bm25_index")
class CreateBM25IndexOp(MigrateOperation):
    def __init__(
        self,
        index_name: str,
        table_name: str,
        expressions: list[str],
        key_field: str,
        *,
        table_schema: str | None = None,
        where: str | None = None,
    ) -> None:
        self.index_name = index_name
        self.table_name = table_name
        self.expressions = expressions
        self.key_field = key_field
        self.table_schema = table_schema
        self.where = where

    @classmethod
    def create_bm25_index(
        cls,
        operations: Operations,
        index_name: str,
        table_name: str,
        expressions: list[str],
        *,
        key_field: str,
        table_schema: str | None = None,
        where: str | None = None,
    ) -> MigrateOperation:
        return operations.invoke(
            cls(
                index_name,
                table_name,
                expressions,
                key_field,
                table_schema=table_schema,
                where=where,
            )
        )


@Operations.implementation_for(CreateBM25IndexOp)
def _create_bm25_index_impl(operations: Operations, operation: CreateBM25IndexOp) -> None:
    expressions_sql = ", ".join(operation.expressions)
    sql = (
        f"CREATE INDEX {_quote_ident(operation.index_name)} "
        f"ON {_quote_qualified(operation.table_schema, operation.table_name)} "
        f"USING bm25 ({expressions_sql}) WITH (key_field={_quote_literal(operation.key_field)})"
    )
    if operation.where is not None:
        sql += f" WHERE {operation.where}"
    operations.execute(sql)


@renderers.dispatch_for(CreateBM25IndexOp)
def _render_create_bm25_index_op(autogen_context, op: CreateBM25IndexOp) -> str:
    parts = [
        repr(op.index_name),
        repr(op.table_name),
        repr(op.expressions),
        f"key_field={op.key_field!r}",
    ]
    if op.table_schema is not None:
        parts.append(f"table_schema={op.table_schema!r}")
    if op.where is not None:
        parts.append(f"where={op.where!r}")
    return f"op.create_bm25_index({', '.join(parts)})"


@Operations.register_operation("drop_bm25_index")
class DropBM25IndexOp(MigrateOperation):
    def __init__(self, index_name: str, if_exists: bool = True, schema: str | None = None) -> None:
        self.index_name = index_name
        self.if_exists = if_exists
        self.schema = schema

    @classmethod
    def drop_bm25_index(
        cls, operations: Operations, index_name: str, if_exists: bool = True, schema: str | None = None
    ) -> MigrateOperation:
        return operations.invoke(cls(index_name=index_name, if_exists=if_exists, schema=schema))


@Operations.implementation_for(DropBM25IndexOp)
def _drop_bm25_index_impl(operations: Operations, operation: DropBM25IndexOp) -> None:
    if_exists_sql = " IF EXISTS" if operation.if_exists else ""
    operations.execute(f"DROP INDEX{if_exists_sql} {_quote_qualified(operation.schema, operation.index_name)}")


@renderers.dispatch_for(DropBM25IndexOp)
def _render_drop_bm25_index_op(autogen_context, op: DropBM25IndexOp) -> str:
    parts = [repr(op.index_name), f"if_exists={op.if_exists!r}"]
    if op.schema is not None:
        parts.append(f"schema={op.schema!r}")
    return f"op.drop_bm25_index({', '.join(parts)})"


@Operations.register_operation("reindex_bm25")
class ReindexBM25Op(MigrateOperation):
    def __init__(self, index_name: str, concurrently: bool = False, schema: str | None = None) -> None:
        self.index_name = index_name
        self.concurrently = concurrently
        self.schema = schema

    @classmethod
    def reindex_bm25(
        cls, operations: Operations, index_name: str, concurrently: bool = False, schema: str | None = None
    ) -> MigrateOperation:
        return operations.invoke(cls(index_name=index_name, concurrently=concurrently, schema=schema))


@Operations.implementation_for(ReindexBM25Op)
def _reindex_bm25_impl(operations: Operations, operation: ReindexBM25Op) -> None:
    concurrently_sql = " CONCURRENTLY" if operation.concurrently else ""
    operations.execute(f"REINDEX INDEX{concurrently_sql} {_quote_qualified(operation.schema, operation.index_name)}")


@renderers.dispatch_for(ReindexBM25Op)
def _render_reindex_bm25_op(autogen_context, op: ReindexBM25Op) -> str:
    parts = [repr(op.index_name), f"concurrently={op.concurrently!r}"]
    if op.schema is not None:
        parts.append(f"schema={op.schema!r}")
    return f"op.reindex_bm25({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Autogenerate comparator
# ---------------------------------------------------------------------------
def _autogen_bm25_meta_indexes(
    metadata, effective_schemas: set[str], *, default_schema: str
) -> dict[tuple[str, str], object]:
    """Return {(schema, index_name): Index} for all BM25 indexes in MetaData."""
    from .indexing import _is_bm25_index

    result: dict[tuple[str, str], object] = {}
    for table in metadata.tables.values():
        schema = table.schema or default_schema
        if schema not in effective_schemas:
            continue
        for index in table.indexes:
            if _is_bm25_index(index):
                result[(schema, index.name)] = index
    return result


def _autogen_bm25_db_indexes(conn, effective_schemas: set[str]) -> dict[tuple[str, str], dict]:
    """Return {(schema, index_name): {table_name, expressions, key_field, where}} from pg_indexes."""
    from .indexing import (
        _extract_key_field,
        _extract_where_clause,
        _introspect_bm25_index_rows,
        _normalize_reloption_value,
    )

    result: dict[tuple[str, str], dict] = {}
    for schema in effective_schemas:
        rows = _introspect_bm25_index_rows(conn, schema_name=schema)
        for row in rows:
            key = (row["schemaname"], row["indexname"])
            index_entry = result.setdefault(
                key,
                {
                    "table_name": row["tablename"],
                    "expressions": [],
                    "key_field": _normalize_reloption_value(row["key_field"]) or "",
                    "where": _extract_where_clause(str(row["indexdef"])),
                },
            )
            index_entry["expressions"].append(str(row["keydef"]))
            if not index_entry["key_field"]:
                index_entry["key_field"] = _extract_key_field(str(row["indexdef"])) or ""
    return result


def _render_bm25_expression(expr: ClauseElement) -> str:
    return str(expr.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))  # type: ignore[no-untyped-call]


def _strip_relation_qualifiers(expr: str, table_name: str) -> str:
    # SQLAlchemy may render column refs as `table.col` in metadata compilation;
    # CREATE INDEX field lists should be table-local expressions.
    stripped = expr.replace(f'"{table_name}".', "")
    stripped = stripped.replace(f"{table_name}.", "")
    return stripped


def _normalize_bm25_expression(expr: str) -> str:
    """Normalize BM25 expression text to reduce false-positive autogen churn."""
    normalized = "".join(expr.split())
    normalized = normalized.replace('"', "")
    normalized = normalized.replace("::text", "")
    return _strip_non_pdb_qualifiers(normalized)


def _strip_non_pdb_qualifiers(expr: str) -> str:
    """Strip relation qualifiers outside SQL string literals.

    Preserves tokenizer namespaces like ``pdb.simple`` and leaves quoted literal
    content untouched (for example regex patterns like ``'run.*'``).
    """
    out: list[str] = []
    i = 0
    in_single = False
    while i < len(expr):
        ch = expr[i]

        if ch == "'":
            out.append(ch)
            # Escaped quote inside a string literal: ''.
            if in_single and i + 1 < len(expr) and expr[i + 1] == "'":
                out.append("'")
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue

        if not in_single and (ch.isalpha() or ch == "_"):
            j = i + 1
            while j < len(expr) and (expr[j].isalnum() or expr[j] == "_"):
                j += 1

            token = expr[i:j]
            if j < len(expr) and expr[j] == ".":
                if token.lower() != "pdb":
                    # Drop relation-like qualifier prefixes, e.g. public.products.
                    i = j + 1
                    continue
                out.append(token)
                out.append(".")
                i = j + 1
                continue

            out.append(token)
            i = j
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def _normalized_expression_list(expressions: list[str]) -> list[str]:
    return [_normalize_bm25_expression(expr) for expr in expressions]


def _normalize_where(clause: str | None) -> str | None:
    """Normalize a WHERE clause for comparison.

    Strips double-quoted identifiers, collapses whitespace, lowercases
    non-literal text, removes ``::text`` casts, and strips relation qualifiers.
    Single-quoted string literals are preserved as-is.
    """
    if clause is None:
        return None
    # Split on single-quoted literals, normalize only the non-literal parts.
    parts = re.split(r"('(?:''|[^'])*')", clause)
    normalized_parts: list[str] = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            # Inside single quotes — preserve exactly.
            normalized_parts.append(part)
        else:
            p = part.replace('"', "")
            p = re.sub(r"\s+", " ", p)
            p = p.lower()
            p = p.replace("::text", "")
            normalized_parts.append(p)
    return _strip_non_pdb_qualifiers("".join(normalized_parts).strip())


def _render_where_from_index(index) -> str | None:
    """Compile the ``postgresql_where`` clause from a SQLAlchemy Index to SQL text."""
    where_clause = index.dialect_options["postgresql"].get("where")
    if where_clause is None:
        return None
    if isinstance(where_clause, ClauseElement):
        return _strip_relation_qualifiers(
            str(
                where_clause.compile(
                    dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
                    compile_kwargs={"literal_binds": True},
                )
            ),
            index.table.name,
        )
    return _strip_relation_qualifiers(str(where_clause), index.table.name)


def _suppress_standard_bm25_ops(upgrade_ops, bm25_names: set[str]) -> None:
    """Remove any standard Alembic CreateIndexOp/DropIndexOp for BM25 indexes."""
    from alembic.operations.ops import CreateIndexOp, DropIndexOp, ModifyTableOps

    # Filter top-level (rare, but defensive)
    upgrade_ops.ops[:] = [
        op
        for op in upgrade_ops.ops
        if not (isinstance(op, (CreateIndexOp, DropIndexOp)) and op.index_name in bm25_names)
    ]
    # Filter inside ModifyTableOps (the normal location for index ops)
    for op in upgrade_ops.ops:
        if isinstance(op, ModifyTableOps):
            op.ops[:] = [
                sub_op
                for sub_op in op.ops
                if not (isinstance(sub_op, (CreateIndexOp, DropIndexOp)) and sub_op.index_name in bm25_names)
            ]


@comparators.dispatch_for("schema", priority=DispatchPriority.LAST)
def _compare_bm25_indexes(autogen_context, upgrade_ops, schemas) -> PriorityDispatchResult:
    """Autogenerate comparator: emit BM25 create/drop ops and suppress incorrect standard ops."""
    conn = autogen_context.connection
    metadata = autogen_context.metadata

    if conn is None or metadata is None:
        return PriorityDispatchResult.CONTINUE

    default_schema: str = conn.dialect.default_schema_name or "public"
    effective_schemas = {s if s is not None else default_schema for s in schemas}

    db_bm25 = _autogen_bm25_db_indexes(conn, effective_schemas)
    meta_bm25 = _autogen_bm25_meta_indexes(metadata, effective_schemas, default_schema=default_schema)

    all_bm25_names = {k[1] for k in db_bm25} | {k[1] for k in meta_bm25}
    if not all_bm25_names:
        return PriorityDispatchResult.CONTINUE

    # Remove any standard CreateIndexOp/DropIndexOp for BM25 indexes since
    # those would render incorrect DDL (BM25Field expressions can't be
    # round-tripped through the standard Inspector → Python code path).
    _suppress_standard_bm25_ops(upgrade_ops, all_bm25_names)

    # Emit drop ops for indexes present in DB but absent from MetaData.
    for key in db_bm25:
        if key not in meta_bm25:
            upgrade_ops.ops.append(DropBM25IndexOp(index_name=key[1], if_exists=True, schema=key[0]))

    # Emit create ops for indexes present in MetaData but absent from DB.
    # Also re-create indexes whose expression list, key_field, or WHERE clause differs from the DB.
    for key, index in meta_bm25.items():
        with_opts = index.dialect_options["postgresql"].get("with") or {}
        key_field = with_opts.get("key_field", "")
        expressions = [
            _strip_relation_qualifiers(_render_bm25_expression(expr), index.table.name) for expr in index.expressions
        ]
        meta_where = _render_where_from_index(index)
        create_op = CreateBM25IndexOp(
            index_name=index.name,
            table_name=index.table.name,
            expressions=expressions,
            key_field=key_field,
            table_schema=key[0],
            where=meta_where,
        )

        if key not in db_bm25:
            upgrade_ops.ops.append(create_op)
        else:
            db = db_bm25[key]
            expressions_changed = _normalized_expression_list(db["expressions"]) != _normalized_expression_list(
                expressions
            )
            key_field_changed = db["key_field"] != key_field
            where_changed = _normalize_where(db.get("where")) != _normalize_where(meta_where)
            if expressions_changed or key_field_changed or where_changed:
                upgrade_ops.ops.append(DropBM25IndexOp(index_name=key[1], if_exists=True, schema=key[0]))
                upgrade_ops.ops.append(create_op)

    return PriorityDispatchResult.CONTINUE
