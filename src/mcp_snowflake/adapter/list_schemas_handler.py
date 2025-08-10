"""ListSchemas EffectHandler implementation."""

from datetime import timedelta

from ..snowflake_client import SnowflakeClient


class ListSchemasEffectHandler:
    """EffectHandler for ListSchemas operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_schemas(self, database: str) -> list[str]:
        """Get list of schemas in a database."""
        query = f"SHOW SCHEMAS IN DATABASE {database}"

        results = await self.client.execute_query(query, timedelta(seconds=10))

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
