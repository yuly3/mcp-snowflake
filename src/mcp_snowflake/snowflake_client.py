"""
Snowflake client for MCP server.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, cast

from snowflake.connector import (
    DataError,
    DictCursor,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    SnowflakeConnection,
)

from expression.contract import contract_async

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
            connection_name=None,
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

    @contract_async(
        known_err=(
            TimeoutError,
            ProgrammingError,
            OperationalError,
            DataError,
            IntegrityError,
            NotSupportedError,
        )
    )
    async def execute_query(
        self,
        query: str,
        query_timeout: timedelta | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a SQL query and return results.

        Parameters
        ----------
        query : str
            SQL query to execute
        query_timeout : timedelta | None, optional
            Query timeout, by default 30 seconds

        Returns
        -------
        list[dict[str, Any]]
            Query results

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
        if query_timeout is None:
            query_timeout = timedelta(seconds=30)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool_executor,
            self._execute_query_sync,
            query,
            query_timeout,
        )
