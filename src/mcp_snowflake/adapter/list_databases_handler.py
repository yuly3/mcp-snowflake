"""ListDatabases EffectHandler implementation."""

import logging
from datetime import timedelta

from kernel.table_metadata import DataBase

from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class ListDatabasesEffectHandler:
    """EffectHandler for ListDatabases operations."""

    def __init__(self, client: SnowflakeClient, query_timeout_seconds: int = 10) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client
        self.query_timeout = timedelta(seconds=query_timeout_seconds)

    async def list_databases(self) -> list[DataBase]:
        """Get list of accessible databases."""
        query = "SHOW DATABASES"

        try:
            results = await self.client.execute_query(query, self.query_timeout)
        except Exception:
            logger.exception(
                "failed to execute list databases operation",
                extra={"query": query},
            )
            raise

        databases: list[DataBase] = []
        for row in results:
            if "name" in row:
                databases.append(DataBase(row["name"]))
            elif "database_name" in row:
                databases.append(DataBase(row["database_name"]))
            else:
                databases.append(DataBase(next(iter(row.values()))))

        return sorted(databases)
