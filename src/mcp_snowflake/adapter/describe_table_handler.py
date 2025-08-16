"""DescribeTable EffectHandler implementation."""

from datetime import timedelta

from kernel.sql_utils import fully_qualified
from kernel.table_metadata import TableColumn, TableInfo

from ..snowflake_client import SnowflakeClient


class DescribeTableEffectHandler:
    """EffectHandler for DescribeTable operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def describe_table(
        self,
        database: str,
        schema: str,
        table: str,
    ) -> TableInfo:
        """Get table structure information."""
        query = f"DESCRIBE TABLE {fully_qualified(database, schema, table)}"

        try:
            results = await self.client.execute_query(query, timedelta(seconds=10))
        except Exception as e:
            raise Exception(
                f"Failed to describe table '{database}.{schema}.{table}': {e!s}"
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
            name=table,
            column_count=len(columns),
            columns=columns,
        )
