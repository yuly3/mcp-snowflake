from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError

from ..handler import EffectListSchemas, ListSchemasArgs, handle_list_schemas
from .base import Tool


class ListSchemasTool(Tool):
    def __init__(self, effect_handler: EffectListSchemas) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "list_schemas"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = ListSchemasArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for list_schemas: {e}",
                )
            ]
        return await handle_list_schemas(args, self.effect_handler)

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name="list_schemas",
            description="Retrieve a list of schemas from a specified database",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to retrieve schemas from",
                    }
                },
                "required": ["database"],
            },
        )
