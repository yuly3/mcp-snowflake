import logging
from collections.abc import Awaitable
from typing import Protocol, TypedDict

from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, Schema, Table

from .name_filter import NameFilter, apply_name_filter

logger = logging.getLogger(__name__)


class ListTablesArgs(BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")
    filter_: NameFilter | None = Field(default=None, alias="filter")


class TablesInfoDict(TypedDict):
    """TypedDict for tables information in JSON response."""

    database: str
    schema: str
    tables: list[str]


class ListTablesJsonResponse(TypedDict):
    """TypedDict for the complete list tables JSON response structure."""

    tables_info: TablesInfoDict


class EffectListTables(Protocol):
    def list_tables(self, database: DataBase, schema: Schema) -> Awaitable[list[Table]]:
        """List tables in a database schema.

        Parameters
        ----------
        database : DataBase
            The database name to list tables from.
        schema : Schema
            The schema name to list tables from.

        Returns
        -------
        Awaitable[list[Table]]
            The list of tables in the schema.

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
) -> ListTablesJsonResponse:
    """Handle list_tables tool call.

    Parameters
    ----------
    args : ListTablesArgs
        The arguments for the list tables operation.
    effect_handler : EffectListTables
        The effect handler for the list tables operation.

    Returns
    -------
    ListTablesJsonResponse
        The structured response containing the tables information.

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
    tables = await effect_handler.list_tables(args.database, args.schema_)
    filtered_tables = apply_name_filter([str(table) for table in tables], args.filter_)

    return ListTablesJsonResponse(
        tables_info={
            "database": str(args.database),
            "schema": str(args.schema_),
            "tables": filtered_tables,
        }
    )
