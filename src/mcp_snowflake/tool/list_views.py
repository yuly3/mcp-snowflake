from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError

from ..handler import EffectListViews, ListViewsArgs, handle_list_views
from .base import Tool


class ListViewsTool(Tool):
    def __init__(self, effect_handler: EffectListViews) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "list_views"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = ListViewsArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for list_views: {e}",
                )
            ]
        return await handle_list_views(args, self.effect_handler)

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Retrieve a list of views from a specified database and schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to retrieve views from",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name to retrieve views from",
                    },
                },
                "required": ["database", "schema"],
            },
        )
