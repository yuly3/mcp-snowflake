from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError

from ..handler import EffectListTables, ListTablesArgs, handle_list_tables
from .base import Tool


class ListTablesTool(Tool):
    def __init__(self, effect_handler: EffectListTables) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "list_tables"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = ListTablesArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for list_tables: {e}",
                )
            ]
        return await handle_list_tables(args, self.effect_handler)

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name="list_tables",
            description="Retrieve a list of tables from a specified database and schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to retrieve tables from",
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Schema name to retrieve tables from",
                    },
                },
                "required": ["database", "schema_name"],
            },
        )
