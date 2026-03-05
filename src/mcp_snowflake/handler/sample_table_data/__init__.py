"""Sample table data handler module."""

import logging
from collections.abc import Awaitable
from typing import Any, Protocol

from more_itertools import first
from pydantic import BaseModel, Field

from cattrs_converter import JsonImmutableConverter
from kernel import DataProcessingResult
from kernel.table_metadata import DataBase, Schema, Table

from ._serializer import (
    CompactSampleTableDataResultSerializer,
    SampleTableDataResult,
    SampleTableDataResultSerializer,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "CompactSampleTableDataResultSerializer",
    "EffectSampleTableData",
    "SampleTableDataArgs",
    "SampleTableDataResult",
    "SampleTableDataResultSerializer",
    "handle_sample_table_data",
]


class SampleTableDataArgs(BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")
    table_: Table = Field(alias="table")
    sample_size: int = Field(default=10, ge=1)
    columns: list[str] = Field(default_factory=list)


class EffectSampleTableData(Protocol):
    def sample_table_data(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        sample_size: int,
        columns: list[str],
    ) -> Awaitable[list[dict[str, Any]]]:
        """Execute sample table data operation.

        Parameters
        ----------
        database : DataBase
            Database name containing the table.
        schema : Schema
            Schema name containing the table.
        table : Table
            Name of the table to sample.
        sample_size : int
            Number of sample rows to retrieve.
        columns : list[str]
            List of column names to retrieve (empty list for all columns).

        Returns
        -------
        Awaitable[list[dict[str, Any]]]
            Raw sample data rows from Snowflake.

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


async def handle_sample_table_data(
    json_converter: JsonImmutableConverter,
    args: SampleTableDataArgs,
    effect_handler: EffectSampleTableData,
) -> SampleTableDataResult:
    """Handle sample_table_data tool call.

    Parameters
    ----------
    json_converter : JsonImmutableConverter
        JSON converter for structuring and unstructuring data.
    args : SampleTableDataArgs
        Arguments for the sample table data operation.
    effect_handler : EffectSampleTableData
        Handler for Snowflake operations.

    Returns
    -------
    SampleTableDataResult
        Format-agnostic sample table data result, ready for serialization.

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
    raw_data = await effect_handler.sample_table_data(
        args.database,
        args.schema_,
        args.table_,
        args.sample_size,
        args.columns,
    )

    result = DataProcessingResult.from_raw_rows(json_converter, raw_data)

    columns = list(first(result.processed_rows, {}).keys())
    return SampleTableDataResult(
        database=args.database,
        schema=args.schema_,
        table=args.table_,
        sample_size=args.sample_size,
        columns=columns,
        rows=result.processed_rows,
        warnings=result.warnings,
    )
