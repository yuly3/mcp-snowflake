"""ListSchemas EffectHandler implementation."""

import logging
from datetime import timedelta

from kernel.sql_utils import quote_ident
from kernel.table_metadata import DataBase, Schema

from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class ListSchemasEffectHandler:
    """EffectHandler for ListSchemas operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_schemas(self, database: DataBase) -> list[Schema]:
        """Get list of schemas in a database."""
        query = f"SHOW SCHEMAS IN DATABASE {quote_ident(database)}"

        try:
            results = await self.client.execute_query(query, timedelta(seconds=10))
        except Exception:
            logger.exception(
                "failed to execute list schemas operation",
                extra={
                    "database": str(database),
                    "query": query,
                },
            )
            raise

        schemas: list[Schema] = []
        for row in results:
            # The schema name is typically in the 'name' field
            if "name" in row:
                schemas.append(Schema(row["name"]))
            elif "schema_name" in row:
                schemas.append(Schema(row["schema_name"]))
            else:
                # If we can't find a standard field, take the first value
                schemas.append(Schema(next(iter(row.values()))))

        return sorted(schemas)
