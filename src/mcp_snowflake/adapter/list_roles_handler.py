"""ListRoles EffectHandler implementation."""

import logging
from datetime import timedelta

from ..handler.list_roles import RoleInfoDict
from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class ListRolesEffectHandler:
    """EffectHandler for ListRoles operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_roles(self) -> list[RoleInfoDict]:
        """Get list of available roles."""
        query = "SHOW ROLES"

        try:
            results = await self.client.execute_query(query, timedelta(seconds=10))
        except Exception:
            logger.exception("failed to execute list roles operation")
            raise

        roles: list[RoleInfoDict] = []
        for row in results:
            roles.append(
                RoleInfoDict(
                    name=row.get("name", ""),
                    owner=row.get("owner"),
                    comment=row.get("comment"),
                )
            )

        return sorted(roles, key=lambda r: r["name"])
