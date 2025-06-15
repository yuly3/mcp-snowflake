"""
Snowflake client for MCP server.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, cast

from snowflake.connector import DictCursor, ProgrammingError, SnowflakeConnection

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

    def _get_connection(self) -> SnowflakeConnection:
        """Create a Snowflake connection."""
        conn_params = {
            "account": self.settings.account,
            "user": self.settings.user,
            "password": self.settings.password.get_secret_value(),
            "warehouse": self.settings.warehouse,
            "role": self.settings.role,
        }

        return SnowflakeConnection(
            connection_name="mcp-snowflake",
            connections_file_path=None,
            **conn_params,
        )

    def _execute_query_sync(
        self,
        query: str,
        timeout: timedelta,
    ) -> list[dict[str, Any]]:
        """Execute a query synchronously and return results."""
        with self._get_connection() as conn, conn.cursor(DictCursor) as cursor:
            timeout_seconds = int(timeout.total_seconds())
            try:
                _ = cursor.execute("begin")
                _ = cursor.execute(query, timeout=timeout_seconds)
                return cast("list[dict[str, Any]]", cursor.fetchall())
            except ProgrammingError as e:
                if e.errno == 604:
                    # see: https://docs.snowflake.com/ja/developer-guide/python-connector/python-connector-example#using-cursor-to-fetch-values
                    logger.exception(
                        f"Query execution timed out after {timeout_seconds} seconds"
                    )
                    raise TimeoutError(
                        f"Query execution timed out after {timeout_seconds} seconds"
                    ) from e
                logger.exception("Query execution error")
                raise
            except Exception:
                logger.exception("Query execution error")
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
                timedelta(seconds=10),
            )
        except TimeoutError:
            raise
        except Exception as e:
            raise Exception(
                f"Failed to retrieve schemas from database '{database}': {e!s}"
            ) from e

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
