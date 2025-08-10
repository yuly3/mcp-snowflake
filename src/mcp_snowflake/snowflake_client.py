"""
Snowflake client for MCP server.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, cast

from snowflake.connector import DictCursor, ProgrammingError, SnowflakeConnection

from .kernel.table_metadata import TableColumn, TableInfo
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

    async def list_schemas(self, database: str) -> list[str]:
        """Get list of schemas in a database."""
        query = f"SHOW SCHEMAS IN DATABASE {database}"

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

    async def list_tables(self, database: str, schema: str) -> list[str]:
        """Get list of tables in a database schema."""
        query = f"SHOW TABLES IN SCHEMA {database}.{schema}"

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
                f"Failed to retrieve tables from schema '{database}.{schema}': {e!s}"
            ) from e

        tables: list[str] = []
        for row in results:
            # The table name is typically in the 'name' field
            if "name" in row:
                tables.append(row["name"])
            elif "table_name" in row:
                tables.append(row["table_name"])
            else:
                # If we can't find a standard field, take the first value
                tables.append(next(iter(row.values())))

        return sorted(tables)

    async def list_views(self, database: str, schema: str) -> list[str]:
        """Get list of views in a database schema."""
        query = f"SHOW VIEWS IN SCHEMA {database}.{schema}"

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
                f"Failed to retrieve views from schema '{database}.{schema}': {e!s}"
            ) from e

        views: list[str] = []
        for row in results:
            # The view name is typically in the 'name' field
            if "name" in row:
                views.append(row["name"])
            elif "view_name" in row:
                views.append(row["view_name"])
            else:
                # If we can't find a standard field, take the first value
                views.append(next(iter(row.values())))

        return sorted(views)

    async def describe_table(
        self,
        database: str,
        schema: str,
        table_name: str,
    ) -> TableInfo:
        """Get table structure information."""
        query = f"DESCRIBE TABLE {database}.{schema}.{table_name}"

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
                f"Failed to describe table '{database}.{schema}.{table_name}': {e!s}"
            ) from e

        # Transform results into structured format
        columns = [
            # Snowflake DESCRIBE TABLE returns columns like:
            # name, type, kind, null?, default, primary key, unique key, check, expression, comment
            TableColumn(
                name=row.get("name", ""),
                data_type=row.get("type", ""),
                nullable=row.get("null?", "Y") == "Y",
                default_value=row.get("default"),
                comment=row.get("comment"),
                ordinal_position=i,
            )
            for i, row in enumerate(results, 1)
        ]

        return TableInfo(
            database=database,
            schema=schema,
            name=table_name,
            column_count=len(columns),
            columns=columns,
        )

    async def sample_table_data(
        self,
        database: str,
        schema: str,
        table_name: str,
        sample_size: int,
        columns: list[str],
    ) -> list[dict[str, Any]]:
        """
        Retrieve sample data from a Snowflake table using SAMPLE ROW clause.

        Parameters
        ----------
        database : str
            Database name
        schema : str
            Schema name
        table_name : str
            Table name
        sample_size : int
            Number of sample rows to retrieve
        columns : list[str] | None
            List of column names to select (if None, selects all columns)

        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries representing sample rows
        """
        column_list = ", ".join(f'"{col}"' for col in columns) if columns else "*"

        query = f"""
        SELECT {column_list}
        FROM "{database}"."{schema}"."{table_name}"
        SAMPLE ROW ({sample_size} ROWS)
        """  # noqa: S608

        logger.info(
            "Executing sample_table_data query for %s.%s.%s with sample_size=%d",
            database,
            schema,
            table_name,
            sample_size,
        )

        timeout = timedelta(seconds=60)
        return await asyncio.get_running_loop().run_in_executor(
            self.thread_pool_executor,
            self._execute_query_sync,
            query,
            timeout,
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
        Exception
            If query execution fails
        """
        if query_timeout is None:
            query_timeout = timedelta(seconds=30)

        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(
                self.thread_pool_executor,
                self._execute_query_sync,
                query,
                query_timeout,
            )
        except TimeoutError:
            raise
        except Exception as e:
            raise Exception(f"Failed to execute query: {e!s}") from e
