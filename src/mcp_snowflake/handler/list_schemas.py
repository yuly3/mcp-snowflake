import logging
from collections.abc import Awaitable
from typing import Protocol, TypedDict

from pydantic import BaseModel

from kernel.table_metadata import DataBase, Schema

from .session_overrides import SessionOverridesMixin

logger = logging.getLogger(__name__)


class ListSchemasArgs(SessionOverridesMixin, BaseModel):
    database: DataBase


class SchemasInfoDict(TypedDict):
    """TypedDict for schemas information in JSON response."""

    database: str
    schemas: list[str]


class ListSchemasJsonResponse(TypedDict):
    """TypedDict for the complete list schemas JSON response structure."""

    schemas_info: SchemasInfoDict


class EffectListSchemas(Protocol):
    def list_schemas(
        self,
        database: DataBase,
        role: str | None = None,
        warehouse: str | None = None,
    ) -> Awaitable[list[Schema]]:
        """List schemas in a database.

        Parameters
        ----------
        database : DataBase
            The database name to list schemas from.
        role : str | None
            Snowflake role to use for this operation.
        warehouse : str | None
            Snowflake warehouse to use for this operation.

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
) -> ListSchemasJsonResponse:
    """Handle list_schemas tool call.

    Parameters
    ----------
    args : ListSchemasArgs
        The arguments for the list schemas operation.
    effect_handler : EffectListSchemas
        The effect handler for the list schemas operation.

    Returns
    -------
    ListSchemasJsonResponse
        The structured response containing the schemas information.

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
    schemas = await effect_handler.list_schemas(
        args.database,
        role=args.role,
        warehouse=args.warehouse,
    )

    return ListSchemasJsonResponse(
        schemas_info={
            "database": str(args.database),
            "schemas": [str(schema) for schema in schemas],
        }
    )
