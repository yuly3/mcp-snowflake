"""List views handler module."""

import logging
from collections.abc import Awaitable
from typing import Protocol

from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, Schema, View

from ..name_filter import NameFilter, apply_name_filter
from ._serializer import (
    CompactListViewsResultSerializer,
    ListViewsResult,
    ListViewsResultSerializer,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "CompactListViewsResultSerializer",
    "EffectListViews",
    "ListViewsArgs",
    "ListViewsResult",
    "ListViewsResultSerializer",
    "handle_list_views",
]


class ListViewsArgs(BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")
    filter_: NameFilter | None = Field(default=None, alias="filter")


class EffectListViews(Protocol):
    def list_views(self, database: DataBase, schema: Schema) -> Awaitable[list[View]]:
        """List views in a database schema.

        Parameters
        ----------
        database : DataBase
            The database name to list views from.
        schema : Schema
            The schema name to list views from.

        Returns
        -------
        Awaitable[list[View]]
            The list of views in the schema.

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


async def handle_list_views(
    args: ListViewsArgs,
    effect_handler: EffectListViews,
) -> ListViewsResult:
    """Handle list_views tool call.

    Parameters
    ----------
    args : ListViewsArgs
        The arguments for the list views operation.
    effect_handler : EffectListViews
        The effect handler for the list views operation.

    Returns
    -------
    ListViewsResult
        Format-agnostic list views result, ready for serialization.

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
    views = await effect_handler.list_views(args.database, args.schema_)
    filtered_views = apply_name_filter([str(view) for view in views], args.filter_)

    return ListViewsResult(
        database=args.database,
        schema=args.schema_,
        views=filtered_views,
    )
