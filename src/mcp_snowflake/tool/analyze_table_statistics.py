from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError

from ..handler import (
    AnalyzeTableStatisticsArgs,
    EffectAnalyzeTableStatistics,
    handle_analyze_table_statistics,
)
from .base import Tool


class AnalyzeTableStatisticsTool(Tool):
    def __init__(self, effect_handler: EffectAnalyzeTableStatistics) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "analyze_table_statistics"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = AnalyzeTableStatisticsArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for analyze_table_statistics: {e}",
                )
            ]
        return await handle_analyze_table_statistics(args, self.effect_handler)

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Analyze table statistics using Snowflake's high-performance approximation functions (APPROX_PERCENTILE, APPROX_TOP_K, APPROX_COUNT_DISTINCT) to efficiently retrieve statistical information for numeric, string, and date columns",
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
                        "description": "Name of the table to analyze",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of column names to analyze (if not specified, all columns will be analyzed)",
                        "default": [],
                    },
                    "top_k_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 10,
                        "description": "Number of top values to retrieve for string columns",
                    },
                },
                "required": ["database", "schema_name", "table_name"],
            },
        )
