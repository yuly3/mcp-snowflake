"""Handler for list_warehouses operation."""

import logging
from collections.abc import Awaitable
from typing import Protocol, TypedDict

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ListWarehousesArgs(BaseModel):
    """Arguments for list_warehouses operation (no arguments required)."""

    pass


class WarehouseInfoDict(TypedDict):
    """TypedDict for warehouse information in JSON response."""

    name: str
    state: str | None
    size: str | None
    owner: str | None
    comment: str | None


class ListWarehousesJsonResponse(TypedDict):
    """TypedDict for the complete list warehouses JSON response structure."""

    warehouses: list[WarehouseInfoDict]


class EffectListWarehouses(Protocol):
    def list_warehouses(self) -> Awaitable[list[WarehouseInfoDict]]:
        """List available warehouses in Snowflake.

        Returns
        -------
        Awaitable[list[WarehouseInfoDict]]
            The list of warehouses with their details.

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


async def handle_list_warehouses(
    args: ListWarehousesArgs,  # noqa: ARG001
    effect_handler: EffectListWarehouses,
) -> ListWarehousesJsonResponse:
    """Handle list_warehouses tool call.

    Parameters
    ----------
    args : ListWarehousesArgs
        The arguments for the list warehouses operation (unused).
    effect_handler : EffectListWarehouses
        The effect handler for the list warehouses operation.

    Returns
    -------
    ListWarehousesJsonResponse
        The structured response containing the warehouses information.

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
    warehouses = await effect_handler.list_warehouses()

    return ListWarehousesJsonResponse(warehouses=warehouses)
