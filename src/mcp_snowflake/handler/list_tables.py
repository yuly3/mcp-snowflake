import logging
from typing import Protocol

import mcp.types as types
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ListTablesArgs(BaseModel):
    database: str
    schema_: str = Field(alias="schema")


class EffectListTables(Protocol):
    async def list_tables(self, database: str, schema: str) -> list[str]: ...


async def handle_list_tables(
    args: ListTablesArgs,
    effect_handler: EffectListTables,
) -> list[types.TextContent]:
    """Handle list_tables tool call."""
    try:
        tables = await effect_handler.list_tables(args.database, args.schema_)
    except Exception as e:
        logger.exception("Error listing tables")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to retrieve tables: {e!s}",
            )
        ]

    table_list = "\n".join([f"- {table}" for table in tables])
    return [
        types.TextContent(
            type="text",
            text=f"Table list for schema '{args.database}.{args.schema_}':\n{table_list}",
        )
    ]
