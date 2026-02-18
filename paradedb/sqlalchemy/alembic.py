from __future__ import annotations

from alembic.autogenerate import comparators, renderers
from alembic.operations import Operations
from alembic.operations.ops import MigrateOperation
from alembic.util import DispatchPriority, PriorityDispatchResult
from sqlalchemy import text


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@Operations.register_operation("create_bm25_index")
class CreateBM25IndexOp(MigrateOperation):
    def __init__(self, index_name: str, table_name: str, fields: list[str], key_field: str) -> None:
        self.index_name = index_name
        self.table_name = table_name
        self.fields = fields
        self.key_field = key_field

    @classmethod
    def create_bm25_index(
        cls,
        operations: Operations,
        index_name: str,
        table_name: str,
        fields: list[str],
        *,
        key_field: str,
    ) -> MigrateOperation:
        return operations.invoke(cls(index_name, table_name, fields, key_field))


@Operations.implementation_for(CreateBM25IndexOp)
def _create_bm25_index_impl(operations: Operations, operation: CreateBM25IndexOp) -> None:
    fields_sql = ", ".join(_quote_ident(field) for field in operation.fields)
    sql = (
        f"CREATE INDEX {_quote_ident(operation.index_name)} ON {_quote_ident(operation.table_name)} "
        f"USING bm25 ({fields_sql}) WITH (key_field={_quote_literal(operation.key_field)})"
    )
    operations.execute(sql)


@renderers.dispatch_for(CreateBM25IndexOp)
def _render_create_bm25_index_op(autogen_context, op: CreateBM25IndexOp) -> str:
    return (
        f"op.create_bm25_index({op.index_name!r}, {op.table_name!r}, {op.fields!r}, "
        f"key_field={op.key_field!r})"
    )


@Operations.register_operation("drop_bm25_index")
class DropBM25IndexOp(MigrateOperation):
    def __init__(self, index_name: str, if_exists: bool = True) -> None:
        self.index_name = index_name
        self.if_exists = if_exists

    @classmethod
    def drop_bm25_index(cls, operations: Operations, index_name: str, if_exists: bool = True) -> MigrateOperation:
        return operations.invoke(cls(index_name=index_name, if_exists=if_exists))


@Operations.implementation_for(DropBM25IndexOp)
def _drop_bm25_index_impl(operations: Operations, operation: DropBM25IndexOp) -> None:
    if_exists_sql = " IF EXISTS" if operation.if_exists else ""
    operations.execute(f"DROP INDEX{if_exists_sql} {_quote_ident(operation.index_name)}")


@renderers.dispatch_for(DropBM25IndexOp)
def _render_drop_bm25_index_op(autogen_context, op: DropBM25IndexOp) -> str:
    return f"op.drop_bm25_index({op.index_name!r}, if_exists={op.if_exists!r})"


@Operations.register_operation("reindex_bm25")
class ReindexBM25Op(MigrateOperation):
    def __init__(self, index_name: str, concurrently: bool = False) -> None:
        self.index_name = index_name
        self.concurrently = concurrently

    @classmethod
    def reindex_bm25(cls, operations: Operations, index_name: str, concurrently: bool = False) -> MigrateOperation:
        return operations.invoke(cls(index_name=index_name, concurrently=concurrently))


@Operations.implementation_for(ReindexBM25Op)
def _reindex_bm25_impl(operations: Operations, operation: ReindexBM25Op) -> None:
    concurrently_sql = " CONCURRENTLY" if operation.concurrently else ""
    operations.execute(f"REINDEX INDEX{concurrently_sql} {_quote_ident(operation.index_name)}")


@renderers.dispatch_for(ReindexBM25Op)
def _render_reindex_bm25_op(autogen_context, op: ReindexBM25Op) -> str:
    return f"op.reindex_bm25({op.index_name!r}, concurrently={op.concurrently!r})"


# ---------------------------------------------------------------------------
# Autogenerate comparator
# ---------------------------------------------------------------------------

def _autogen_bm25_meta_indexes(metadata, effective_schemas: set[str]) -> dict[tuple[str, str], object]:
    """Return {(schema, index_name): Index} for all BM25 indexes in MetaData."""
    from .indexing import _is_bm25_index

    result: dict[tuple[str, str], object] = {}
    for table in metadata.tables.values():
        schema = table.schema or next(iter(effective_schemas), "public")
        if schema not in effective_schemas:
            continue
        for index in table.indexes:
            if _is_bm25_index(index):
                result[(schema, index.name)] = index
    return result


def _autogen_bm25_db_indexes(conn, effective_schemas: set[str]) -> dict[tuple[str, str], dict]:
    """Return {(schema, index_name): {table_name, fields, key_field}} from pg_indexes."""
    from .indexing import _extract_bm25_field_list, _extract_field_name, _extract_key_field

    result: dict[tuple[str, str], dict] = {}
    for schema in effective_schemas:
        rows = conn.execute(
            text(
                """
                SELECT schemaname, tablename, indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = :schema
                  AND indexdef ILIKE '%USING bm25%'
                ORDER BY indexname
                """
            ),
            {"schema": schema},
        ).fetchall()
        for row in rows:
            raw_fields = _extract_bm25_field_list(row.indexdef)
            fields = [f for f in (_extract_field_name(rf) for rf in raw_fields) if f is not None]
            result[(row.schemaname, row.indexname)] = {
                "table_name": row.tablename,
                "fields": fields,
                "key_field": _extract_key_field(row.indexdef) or "",
            }
    return result


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
                if not (
                    isinstance(sub_op, (CreateIndexOp, DropIndexOp))
                    and sub_op.index_name in bm25_names
                )
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
    meta_bm25 = _autogen_bm25_meta_indexes(metadata, effective_schemas)

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
            upgrade_ops.ops.append(DropBM25IndexOp(index_name=key[1], if_exists=True))

    # Emit create ops for indexes present in MetaData but absent from DB.
    # Also re-create indexes whose field list or key_field differs from the DB.
    for key, index in meta_bm25.items():
        from .indexing import _bm25_field_name

        with_opts = index.dialect_options["postgresql"].get("with") or {}
        key_field = with_opts.get("key_field", "")
        fields = [f for f in (_bm25_field_name(expr) for expr in index.expressions) if f is not None]

        if key not in db_bm25:
            upgrade_ops.ops.append(
                CreateBM25IndexOp(
                    index_name=index.name,
                    table_name=index.table.name,
                    fields=fields,
                    key_field=key_field,
                )
            )
        else:
            db = db_bm25[key]
            if db["fields"] != fields or db["key_field"] != key_field:
                # Index configuration changed: drop the old one, create the new one.
                upgrade_ops.ops.append(DropBM25IndexOp(index_name=key[1], if_exists=True))
                upgrade_ops.ops.append(
                    CreateBM25IndexOp(
                        index_name=index.name,
                        table_name=index.table.name,
                        fields=fields,
                        key_field=key_field,
                    )
                )

    return PriorityDispatchResult.CONTINUE
