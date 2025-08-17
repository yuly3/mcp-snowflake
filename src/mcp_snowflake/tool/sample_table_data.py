import json
from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from cattrs_converter import JsonImmutableConverter
from expression.contract import ContractViolationError

from ..handler import (
    EffectSampleTableData,
    SampleTableDataArgs,
    handle_sample_table_data,
)
from .base import Tool


class SampleTableDataTool(Tool):
    def __init__(
        self,
        json_converter: JsonImmutableConverter,
        effect_handler: EffectSampleTableData,
    ) -> None:
        self.json_converter = json_converter
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

        try:
            result = await handle_sample_table_data(
                self.json_converter,
                args,
                self.effect_handler,
            )
        except TimeoutError as e:
            text = f"Error: Query timed out: {e}"
        except ProgrammingError as e:
            text = f"Error: SQL syntax error or other programming error: {e}"
        except OperationalError as e:
            text = f"Error: Database operation related error: {e}"
        except DataError as e:
            text = f"Error: Data processing related error: {e}"
        except IntegrityError as e:
            text = f"Error: Referential integrity constraint violation: {e}"
        except NotSupportedError as e:
            text = f"Error: Unsupported database feature used: {e}"
        except ContractViolationError as e:
            text = f"Error: Unexpected error: {e}"
        else:
            text = json.dumps(result, indent=2)
        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Retrieve sample data from a specified table using SAMPLE ROW clause",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name containing the table",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name containing the table",
                    },
                    "table": {
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
                "required": ["database", "schema", "table"],
            },
        )
