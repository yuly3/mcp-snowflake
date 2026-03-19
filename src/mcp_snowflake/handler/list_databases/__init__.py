"""List databases handler module."""

import logging
from collections.abc import Awaitable
from typing import Protocol

from kernel.table_metadata import DataBase

from ._serializer import (
    CompactListDatabasesResultSerializer,
    ListDatabasesResult,
    ListDatabasesResultSerializer,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "CompactListDatabasesResultSerializer",
    "EffectListDatabases",
    "ListDatabasesResult",
    "ListDatabasesResultSerializer",
    "handle_list_databases",
]


class EffectListDatabases(Protocol):
    def list_databases(self) -> Awaitable[list[DataBase]]:
        """List accessible databases.

        Returns
        -------
        Awaitable[list[DataBase]]
            The list of databases.

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


async def handle_list_databases(
    effect_handler: EffectListDatabases,
) -> ListDatabasesResult:
    """Handle list_databases tool call.

    Parameters
    ----------
    effect_handler : EffectListDatabases
        The effect handler for the list databases operation.

    Returns
    -------
    ListDatabasesResult
        Format-agnostic list databases result, ready for serialization.

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
    databases = await effect_handler.list_databases()

    return ListDatabasesResult(
        databases=[str(db) for db in databases],
    )
