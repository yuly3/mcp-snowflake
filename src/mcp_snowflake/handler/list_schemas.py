import logging
from collections.abc import Sequence
from typing import Protocol

import mcp.types as types
from pydantic import BaseModel

from kernel.table_metadata import DataBase, Schema

logger = logging.getLogger(__name__)


class ListSchemasArgs(BaseModel):
    database: DataBase


class EffectListSchemas(Protocol):
    async def list_schemas(self, database: DataBase) -> list[Schema]: ...


async def handle_list_schemas(
    args: ListSchemasArgs,
    effect_handler: EffectListSchemas,
) -> Sequence[types.TextContent]:
    """Handle list_schemas tool call."""
    try:
        schemas = await effect_handler.list_schemas(args.database)
    except Exception as e:
        logger.exception("Error listing schemas")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to retrieve schemas: {e!s}",
            )
        ]

    schema_list = "\n".join([f"- {schema}" for schema in schemas])
    return [
        types.TextContent(
            type="text",
            text=f"Schema list for database '{args.database}':\n{schema_list}",
        )
    ]
