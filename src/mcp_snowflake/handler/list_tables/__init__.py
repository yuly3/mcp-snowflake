"""List tables handler module."""

import logging
from collections.abc import Awaitable
from typing import Protocol

from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, ObjectKind, Schema, SchemaObject

from ..name_filter import ListObjectsFilter, apply_list_objects_filter
from ._serializer import (
    CompactListTablesResultSerializer,
    ListTablesResult,
    ListTablesResultSerializer,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "CompactListTablesResultSerializer",
    "EffectListTables",
    "ListTablesArgs",
    "ListTablesResult",
    "ListTablesResultSerializer",
    "handle_list_tables",
]


class ListTablesArgs(BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")
    filter_: ListObjectsFilter | None = Field(default=None, alias="filter", discriminator="type_")


class EffectListTables(Protocol):
    def list_objects(self, database: DataBase, schema: Schema) -> Awaitable[list[SchemaObject]]:
        """List objects (tables and views) in a database schema.

        Parameters
        ----------
        database : DataBase
            The database name to list objects from.
        schema : Schema
            The schema name to list objects from.

        Returns
        -------
        Awaitable[list[SchemaObject]]
            The list of objects in the schema.

        Raises
        ------
        TimeoutError
            If query execution times out
        ProgrammingError
            SQL syntax errors or other programming errors
        OperationalError
            Database operation related errors
        DataError
            Data processing related errors
        IntegrityError
            Referential integrity constraint violations
        NotSupportedError
            When an unsupported database feature is used
        """
        ...


async def handle_list_tables(
    args: ListTablesArgs,
    effect_handler: EffectListTables,
) -> ListTablesResult:
    """Handle list_tables tool call.

    Parameters
    ----------
    args : ListTablesArgs
        The arguments for the list tables operation.
    effect_handler : EffectListTables
        The effect handler for the list tables operation.

    Returns
    -------
    ListTablesResult
        Format-agnostic list tables result, ready for serialization.

    Raises
    ------
    TimeoutError
        If query execution times out
    ProgrammingError
        SQL syntax errors or other programming errors
    OperationalError
        Database operation related errors
    DataError
        Data processing related errors
    IntegrityError
        Referential integrity constraint violations
    NotSupportedError
        When an unsupported database feature is used
    """
    objects = await effect_handler.list_objects(args.database, args.schema_)
    filtered = apply_list_objects_filter(objects, args.filter_)

    tables = [obj.name for obj in filtered if obj.kind == ObjectKind.TABLE]
    views = [obj.name for obj in filtered if obj.kind == ObjectKind.VIEW]

    return ListTablesResult(
        database=args.database,
        schema=args.schema_,
        tables=tables,
        views=views,
    )
