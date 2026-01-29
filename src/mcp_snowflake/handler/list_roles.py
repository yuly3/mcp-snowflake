"""Handler for list_roles operation."""

import logging
from collections.abc import Awaitable
from typing import Protocol, TypedDict

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ListRolesArgs(BaseModel):
    """Arguments for list_roles operation (no arguments required)."""

    pass


class RoleInfoDict(TypedDict):
    """TypedDict for role information in JSON response."""

    name: str
    owner: str | None
    comment: str | None


class ListRolesJsonResponse(TypedDict):
    """TypedDict for the complete list roles JSON response structure."""

    roles: list[RoleInfoDict]


class EffectListRoles(Protocol):
    def list_roles(self) -> Awaitable[list[RoleInfoDict]]:
        """List available roles in Snowflake.

        Returns
        -------
        Awaitable[list[RoleInfoDict]]
            The list of roles with their details.

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


async def handle_list_roles(
    args: ListRolesArgs,  # noqa: ARG001
    effect_handler: EffectListRoles,
) -> ListRolesJsonResponse:
    """Handle list_roles tool call.

    Parameters
    ----------
    args : ListRolesArgs
        The arguments for the list roles operation (unused).
    effect_handler : EffectListRoles
        The effect handler for the list roles operation.

    Returns
    -------
    ListRolesJsonResponse
        The structured response containing the roles information.

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
    roles = await effect_handler.list_roles()

    return ListRolesJsonResponse(roles=roles)
