"""ListTables EffectHandler implementation."""

from datetime import timedelta

from ..snowflake_client import SnowflakeClient


class ListTablesEffectHandler:
    """EffectHandler for ListTables operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_tables(self, database: str, schema: str) -> list[str]:
        """Get list of tables in a database schema."""
        query = f"SHOW TABLES IN SCHEMA {database}.{schema}"

        results = await self.client.execute_query(query, timedelta(seconds=10))

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
