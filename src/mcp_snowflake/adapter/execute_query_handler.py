"""ExecuteQuery EffectHandler implementation."""

from datetime import timedelta
from typing import Any

from ..snowflake_client import SnowflakeClient


class ExecuteQueryEffectHandler:
    """EffectHandler for ExecuteQuery operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def execute_query(
        self,
        query: str,
        query_timeout: timedelta | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a SQL query and return results."""
        return await self.client.execute_query(query, query_timeout)
