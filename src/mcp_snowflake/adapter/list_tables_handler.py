"""ListTables EffectHandler implementation."""

import logging
from datetime import timedelta

from kernel.sql_utils import quote_ident
from kernel.table_metadata import DataBase, ObjectKind, Schema, SchemaObject

from ..handler.errors import MissingResponseColumnError
from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS = frozenset({"name", "kind"})


class ListTablesEffectHandler:
    """EffectHandler for ListTables operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_objects(self, database: DataBase, schema: Schema) -> list[SchemaObject]:
        """Get list of objects (tables and views) in a database schema."""
        query = f"SHOW OBJECTS IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}"

        try:
            results = await self.client.execute_query(query, timedelta(seconds=10))
        except Exception:
            logger.exception(
                "failed to execute list objects operation",
                extra={
                    "database": str(database),
                    "schema": str(schema),
                    "query": query,
                },
            )
            raise

        objects: list[SchemaObject] = []
        for row in results:
            missing = _REQUIRED_COLUMNS - row.keys()
            if missing:
                raise MissingResponseColumnError(
                    f"SHOW OBJECTS response is missing required columns: {', '.join(sorted(missing))}"
                )

            match row["kind"]:
                case ObjectKind.TABLE.value:
                    kind = ObjectKind.TABLE
                case ObjectKind.VIEW.value:
                    kind = ObjectKind.VIEW
                case _:
                    continue

            objects.append(SchemaObject(name=row["name"], kind=kind))

        return sorted(objects, key=lambda obj: (obj.kind.value, obj.name))
