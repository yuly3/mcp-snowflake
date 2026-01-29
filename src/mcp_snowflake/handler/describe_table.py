import logging
from collections.abc import Awaitable
from typing import Protocol, TypedDict

from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, Schema, Table, TableInfo

from .session_overrides import SessionOverridesMixin

logger = logging.getLogger(__name__)


class ColumnDict(TypedDict):
    """TypedDict for column information in JSON response."""

    name: str
    data_type: str
    nullable: bool
    default_value: str | None
    comment: str | None
    ordinal_position: int


class TableInfoDict(TypedDict):
    """TypedDict for table information in JSON response."""

    database: DataBase
    schema: Schema
    name: str
    column_count: int
    columns: list[ColumnDict]


class TableJsonResponse(TypedDict):
    """TypedDict for the complete table JSON response structure."""

    table_info: TableInfoDict


class DescribeTableArgs(SessionOverridesMixin, BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")
    table_: Table = Field(alias="table")


class EffectDescribeTable(Protocol):
    def describe_table(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        role: str | None = None,
        warehouse: str | None = None,
    ) -> Awaitable[TableInfo]:
        """Describe a table in the database.

        Parameters
        ----------
        database : DataBase
            The database containing the table.
        schema : Schema
            The schema containing the table.
        table : Table
            The table to describe.
        role : str | None
            Snowflake role to use for this operation.
        warehouse : str | None
            Snowflake warehouse to use for this operation.

        Returns
        -------
        Awaitable[TableInfo]
            The information about the table.

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


async def handle_describe_table(
    args: DescribeTableArgs,
    effect_handler: EffectDescribeTable,
) -> TableJsonResponse:
    """Handle describe_table tool call.

    Parameters
    ----------
    args : DescribeTableArgs
        The arguments for describing the table.
    effect_handler : EffectDescribeTable
        The effect handler for describing the table.

    Returns
    -------
    TableJsonResponse
        The JSON response containing the table information.

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
    table_data = await effect_handler.describe_table(
        args.database,
        args.schema_,
        args.table_,
        role=args.role,
        warehouse=args.warehouse,
    )

    columns_dict: list[ColumnDict] = [
        {
            "name": col.name,
            "data_type": col.data_type.raw_type,
            "nullable": col.nullable,
            "default_value": col.default_value,
            "comment": col.comment,
            "ordinal_position": col.ordinal_position,
        }
        for col in table_data.columns
    ]

    return TableJsonResponse(
        table_info={
            "database": table_data.database,
            "schema": table_data.schema,
            "name": table_data.name,
            "column_count": table_data.column_count,
            "columns": columns_dict,
        }
    )
