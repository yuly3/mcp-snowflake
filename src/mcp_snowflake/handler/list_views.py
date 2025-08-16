import logging
from typing import Protocol

import mcp.types as types
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ListViewsArgs(BaseModel):
    database: str
    schema_: str = Field(alias="schema")


class EffectListViews(Protocol):
    async def list_views(self, database: str, schema: str) -> list[str]: ...


async def handle_list_views(
    args: ListViewsArgs,
    effect_handler: EffectListViews,
) -> list[types.TextContent]:
    """Handle list_views tool call."""
    try:
        views = await effect_handler.list_views(args.database, args.schema_)
    except Exception as e:
        logger.exception("Error listing views")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to retrieve views: {e!s}",
            )
        ]

    view_list = "\n".join([f"- {view}" for view in views])
    return [
        types.TextContent(
            type="text",
            text=f"View list for schema '{args.database}.{args.schema_}':\n{view_list}",
        )
    ]
