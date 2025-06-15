"""
Snowflake client for MCP server.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

from snowflake.connector import DictCursor, SnowflakeConnection

from .settings import SnowflakeSettings

logger = logging.getLogger(__name__)


class SnowflakeClient:
    """Snowflake database client."""

    def __init__(
        self,
        thread_pool_executor: ThreadPoolExecutor,
        settings: SnowflakeSettings,
    ) -> None:
        """Initialize Snowflake client with environment variables."""
        self.thread_pool_executor = thread_pool_executor
        self.settings = settings

        # Validate required environment variables
        if not all([self.settings.account, self.settings.user, self.settings.password]):
            msg = """Required environment variables are not set. Please configure the following:
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
Optional:
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_ROLE"""
            raise ValueError(msg)

    def _get_connection(self) -> SnowflakeConnection:
        """Create a Snowflake connection."""
        conn_params = {
            "account": self.settings.account,
            "user": self.settings.user,
            "password": self.settings.password.get_secret_value(),
        }

        if self.settings.warehouse:
            conn_params["warehouse"] = self.settings.warehouse
        if self.settings.role:
            conn_params["role"] = self.settings.role

        return SnowflakeConnection(
            connection_name="mcp-snowflake",
            connections_file_path=None,
            **conn_params,
        )

    def _execute_query_sync(self, query: str) -> list[dict[str, Any]]:
        """Execute a query synchronously and return results."""
        with self._get_connection() as conn, conn.cursor(DictCursor) as cursor:
            try:
                _ = cursor.execute(query)
                return cast("list[dict[str, Any]]", cursor.fetchall())
            except Exception as e:
                logger.exception(f"Query execution error: {e}")
                raise

    async def list_schemas(self, database: str) -> list[str]:
        """Get list of schemas in a database."""
        query = f"SHOW SCHEMAS IN DATABASE {database}"

        # Run the synchronous query in a thread pool
        loop = asyncio.get_event_loop()
        try:
            results = await loop.run_in_executor(
                self.thread_pool_executor,
                self._execute_query_sync,
                query,
            )

            schemas: list[str] = []
            for row in results:
                # The schema name is typically in the 'name' field
                if "name" in row:
                    schemas.append(row["name"])
                elif "schema_name" in row:
                    schemas.append(row["schema_name"])
                else:
                    # If we can't find a standard field, take the first value
                    schemas.append(next(iter(row.values())))

            return sorted(schemas)

        except Exception as e:
            logger.exception(f"Schema list retrieval error: {e}")
            raise Exception(
                f"Failed to retrieve schemas from database '{database}': {e!s}"
            ) from e
