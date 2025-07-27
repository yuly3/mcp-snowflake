from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError

from ..handler import EffectExecuteQuery, ExecuteQueryArgs, handle_execute_query
from .base import Tool


class ExecuteQueryTool(Tool):
    def __init__(self, effect_handler: EffectExecuteQuery) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "execute_query"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = ExecuteQueryArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for execute_query: {e}",
                )
            ]
        return await handle_execute_query(args, self.effect_handler)

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name="execute_query",
            description="Execute a read-only SQL query and return the results. Only SELECT, SHOW, DESCRIBE, EXPLAIN and similar read operations are allowed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query to execute (read operations only)",
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Query timeout in seconds (default: 30)",
                        "default": 30,
                        "minimum": 1,
                        "maximum": 300,
                    },
                },
                "required": ["sql"],
            },
        )
