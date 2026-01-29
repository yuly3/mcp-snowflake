"""ListTables EffectHandler implementation."""

import logging
from datetime import timedelta

from kernel.sql_utils import quote_ident
from kernel.table_metadata import DataBase, Schema, Table

from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class ListTablesEffectHandler:
    """EffectHandler for ListTables operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_tables(
        self,
        database: DataBase,
        schema: Schema,
        role: str | None = None,
        warehouse: str | None = None,
    ) -> list[Table]:
        """Get list of tables in a database schema."""
        query = f"SHOW TABLES IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}"

        try:
            results = await self.client.execute_query(
                query,
                timedelta(seconds=10),
                role=role,
                warehouse=warehouse,
            )
        except Exception:
            logger.exception(
                "failed to execute list tables operation",
                extra={
                    "database": str(database),
                    "schema": str(schema),
                    "query": query,
                },
            )
            raise

        tables: list[Table] = []
        for row in results:
            # The table name is typically in the 'name' field
            if "name" in row:
                tables.append(Table(row["name"]))
            elif "table_name" in row:
                tables.append(Table(row["table_name"]))
            else:
                # If we can't find a standard field, take the first value
                tables.append(Table(next(iter(row.values()))))

        return sorted(tables)
