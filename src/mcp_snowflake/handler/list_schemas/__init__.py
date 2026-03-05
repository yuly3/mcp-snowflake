"""List schemas handler module."""

import logging
from collections.abc import Awaitable
from typing import Protocol

from pydantic import BaseModel

from kernel.table_metadata import DataBase, Schema

from ._serializer import (
    CompactListSchemasResultSerializer,
    ListSchemasResult,
    ListSchemasResultSerializer,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "CompactListSchemasResultSerializer",
    "EffectListSchemas",
    "ListSchemasArgs",
    "ListSchemasResult",
    "ListSchemasResultSerializer",
    "handle_list_schemas",
]


class ListSchemasArgs(BaseModel):
    database: DataBase


class EffectListSchemas(Protocol):
    def list_schemas(self, database: DataBase) -> Awaitable[list[Schema]]:
        """List schemas in a database.

        Parameters
        ----------
        database : DataBase
            The database name to list schemas from.

        Returns
        -------
        Awaitable[list[Schema]]
            The list of schemas in the database.

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


async def handle_list_schemas(
    args: ListSchemasArgs,
    effect_handler: EffectListSchemas,
) -> ListSchemasResult:
    """Handle list_schemas tool call.

    Parameters
    ----------
    args : ListSchemasArgs
        The arguments for the list schemas operation.
    effect_handler : EffectListSchemas
        The effect handler for the list schemas operation.

    Returns
    -------
    ListSchemasResult
        Format-agnostic list schemas result, ready for serialization.

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
    schemas = await effect_handler.list_schemas(args.database)

    return ListSchemasResult(
        database=args.database,
        schemas=[str(schema) for schema in schemas],
    )
