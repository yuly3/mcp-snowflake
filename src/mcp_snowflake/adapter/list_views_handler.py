"""ListViews EffectHandler implementation."""

from datetime import timedelta

from kernel.sql_utils import quote_ident
from kernel.table_metadata import DataBase, Schema, View

from ..snowflake_client import SnowflakeClient


class ListViewsEffectHandler:
    """EffectHandler for ListViews operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_views(self, database: DataBase, schema: Schema) -> list[View]:
        """Get list of views in a database schema."""
        query = f"SHOW VIEWS IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}"

        results = await self.client.execute_query(query, timedelta(seconds=10))

        views: list[View] = []
        for row in results:
            # The view name is typically in the 'name' field
            if "name" in row:
                views.append(View(row["name"]))
            elif "view_name" in row:
                views.append(View(row["view_name"]))
            else:
                # If we can't find a standard field, take the first value
                views.append(View(next(iter(row.values()))))

        return sorted(views)
