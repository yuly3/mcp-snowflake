import logging
from collections.abc import Awaitable
from typing import Protocol, TypedDict

from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, Schema, View

from .session_overrides import SessionOverridesMixin

logger = logging.getLogger(__name__)


class ListViewsArgs(SessionOverridesMixin, BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")


class ViewsInfoDict(TypedDict):
    """TypedDict for views information in JSON response."""

    database: str
    schema: str
    views: list[str]


class ListViewsJsonResponse(TypedDict):
    """TypedDict for the complete list views JSON response structure."""

    views_info: ViewsInfoDict


class EffectListViews(Protocol):
    def list_views(
        self,
        database: DataBase,
        schema: Schema,
        role: str | None = None,
        warehouse: str | None = None,
    ) -> Awaitable[list[View]]:
        """List views in a database schema.

        Parameters
        ----------
        database : DataBase
            The database name to list views from.
        schema : Schema
            The schema name to list views from.
        role : str | None
            Snowflake role to use for this operation.
        warehouse : str | None
            Snowflake warehouse to use for this operation.

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
) -> ListViewsJsonResponse:
    """Handle list_views tool call.

    Parameters
    ----------
    args : ListViewsArgs
        The arguments for the list views operation.
    effect_handler : EffectListViews
        The effect handler for the list views operation.

    Returns
    -------
    ListViewsJsonResponse
        The structured response containing the views information.

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
    views = await effect_handler.list_views(
        args.database,
        args.schema_,
        role=args.role,
        warehouse=args.warehouse,
    )

    return ListViewsJsonResponse(
        views_info={
            "database": str(args.database),
            "schema": str(args.schema_),
            "views": [str(view) for view in views],
        }
    )
