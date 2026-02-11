from __future__ import annotations

from alembic.operations import Operations
from alembic.operations.ops import MigrateOperation


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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
        f"USING bm25 ({fields_sql}) WITH (key_field='{operation.key_field}')"
    )
    operations.execute(sql)


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
