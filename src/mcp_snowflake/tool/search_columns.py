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

from expression.contract import ContractViolationError

from ..handler import (
    CompactSearchColumnsResultSerializer,
    EffectSearchColumns,
    MissingResponseColumnError,
    SearchColumnsArgs,
    handle_search_columns,
)
from .base import Tool


class SearchColumnsTool(Tool):
    def __init__(self, effect_handler: EffectSearchColumns) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "search_columns"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = SearchColumnsArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for search_columns: {e}",
                )
            ]

        try:
            result = await handle_search_columns(args, self.effect_handler)
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
        except MissingResponseColumnError as e:
            text = f"Error: Missing required columns in Snowflake response: {e}"
        except ContractViolationError as e:
            text = f"Error: Unexpected error: {e}"
        else:
            text = result.serialize_with(CompactSearchColumnsResultSerializer())
        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Search for columns across tables in a database by column name pattern and/or data type. At least one of 'column_name_pattern' or 'data_type' must be provided.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to search in",
                    },
                    "column_name_pattern": {
                        "type": "string",
                        "description": "Column name ILIKE pattern (e.g. '%unit_id%')",
                    },
                    "data_type": {
                        "type": "string",
                        "description": "Data type to filter by (e.g. 'VARIANT', 'NUMBER')",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name to filter by",
                    },
                    "table_name_pattern": {
                        "type": "string",
                        "description": "Table name ILIKE pattern (e.g. '%ORDERS%')",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 50,
                        "description": "Maximum number of tables to return (default: 50)",
                    },
                },
                "required": ["database"],
            },
        )
