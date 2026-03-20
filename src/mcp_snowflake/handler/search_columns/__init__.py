"""Search columns handler module."""

from collections.abc import Awaitable, Sequence
from typing import Protocol

from pydantic import BaseModel, Field, model_validator

from kernel.table_metadata import DataBase

from ._serializer import (
    CompactSearchColumnsResultSerializer,
    SearchColumnsResult,
    SearchColumnsResultSerializer,
    SearchColumnsTableEntry,
)

# Public API exports
__all__ = [
    "CompactSearchColumnsResultSerializer",
    "EffectSearchColumns",
    "SearchColumnsArgs",
    "SearchColumnsResult",
    "SearchColumnsResultSerializer",
    "SearchColumnsTableEntry",
    "handle_search_columns",
]


class SearchColumnsArgs(BaseModel):
    """Arguments for searching columns across a database."""

    database: DataBase
    column_name_pattern: str | None = Field(default=None)
    data_type: str | None = Field(default=None)
    schema_: str | None = Field(default=None, alias="schema")
    table_name_pattern: str | None = Field(default=None)
    limit: int = Field(default=50, ge=1, le=200)

    @model_validator(mode="after")
    def validate_at_least_one_filter(self) -> "SearchColumnsArgs":
        """At least one of column_name_pattern or data_type must be provided."""
        if self.column_name_pattern is None and self.data_type is None:
            raise ValueError("At least one of 'column_name_pattern' or 'data_type' must be provided")
        return self


class EffectSearchColumns(Protocol):
    def search_columns(
        self,
        database: DataBase,
        column_name_pattern: str | None,
        data_type: str | None,
        schema: str | None,
        table_name_pattern: str | None,
        limit: int,
    ) -> Awaitable[Sequence[SearchColumnsTableEntry]]:
        """Search columns in a database.

        Parameters
        ----------
        database : DataBase
            The database to search in.
        column_name_pattern : str | None
            ILIKE pattern for column names.
        data_type : str | None
            Exact data type filter.
        schema : str | None
            Schema name filter.
        table_name_pattern : str | None
            ILIKE pattern for table names.
        limit : int
            Maximum number of tables to return.

        Returns
        -------
        Awaitable[Sequence[SearchColumnsTableEntry]]
            The matched table entries.

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


async def handle_search_columns(
    args: SearchColumnsArgs,
    effect_handler: EffectSearchColumns,
) -> SearchColumnsResult:
    """Handle search_columns tool call.

    Parameters
    ----------
    args : SearchColumnsArgs
        The arguments for the search columns operation.
    effect_handler : EffectSearchColumns
        The effect handler for the search columns operation.

    Returns
    -------
    SearchColumnsResult
        Format-agnostic search columns result, ready for serialization.

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
    tables = await effect_handler.search_columns(
        args.database,
        args.column_name_pattern,
        args.data_type,
        args.schema_,
        args.table_name_pattern,
        args.limit,
    )

    return SearchColumnsResult(
        database=args.database,
        tables=list(tables),
    )
