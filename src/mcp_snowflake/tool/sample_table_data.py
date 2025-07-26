from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError

from ..handler import (
    EffectSampleTableData,
    SampleTableDataArgs,
    handle_sample_table_data,
)
from .base import Tool


class SampleTableDataTool(Tool):
    def __init__(self, effect_handler: EffectSampleTableData) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "sample_table_data"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = SampleTableDataArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for sample_table_data: {e}",
                )
            ]
        return await handle_sample_table_data(args, self.effect_handler)

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name="sample_table_data",
            description="Retrieve sample data from a specified table using SAMPLE ROW clause",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name containing the table",
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Schema name containing the table",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to sample",
                    },
                    "sample_size": {
                        "type": "integer",
                        "description": "Number of sample rows to retrieve (default: 10)",
                        "default": 10,
                        "minimum": 1,
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of column names to retrieve (if not specified, all columns will be retrieved)",
                    },
                },
                "required": ["database", "schema_name", "table_name"],
            },
        )
