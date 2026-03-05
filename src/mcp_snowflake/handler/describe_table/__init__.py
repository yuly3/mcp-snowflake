"""Describe table handler module."""

import logging
from collections.abc import Awaitable
from typing import Protocol

from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, Schema, Table, TableInfo

from ._serializer import (
    CompactDescribeTableResultSerializer,
    DescribeTableResult,
    DescribeTableResultSerializer,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "CompactDescribeTableResultSerializer",
    "DescribeTableArgs",
    "DescribeTableResult",
    "DescribeTableResultSerializer",
    "EffectDescribeTable",
    "handle_describe_table",
]


class DescribeTableArgs(BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")
    table_: Table = Field(alias="table")


class EffectDescribeTable(Protocol):
    def describe_table(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
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
) -> DescribeTableResult:
    """Handle describe_table tool call.

    Parameters
    ----------
    args : DescribeTableArgs
        The arguments for describing the table.
    effect_handler : EffectDescribeTable
        The effect handler for describing the table.

    Returns
    -------
    DescribeTableResult
        Format-agnostic describe table result, ready for serialization.

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
    )

    return DescribeTableResult(
        database=table_data.database,
        schema=table_data.schema,
        name=table_data.name,
        column_count=table_data.column_count,
        columns=table_data.columns,
    )
