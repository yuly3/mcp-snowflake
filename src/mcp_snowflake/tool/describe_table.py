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

from expression.contract import ContractViolationError

from ..handler import DescribeTableArgs, EffectDescribeTable, handle_describe_table
from ..handler.session_overrides import SESSION_OVERRIDE_PROPERTIES
from .base import Tool


class DescribeTableTool(Tool):
    def __init__(self, effect_handler: EffectDescribeTable) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "describe_table"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = DescribeTableArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for describe_table: {e}",
                )
            ]

        try:
            result = await handle_describe_table(args, self.effect_handler)
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
            description="Retrieve the structure (columns, data types, etc.) of a specified table",
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
                        "description": "Name of the table to describe",
                    },
                    **SESSION_OVERRIDE_PROPERTIES,
                },
                "required": ["database", "schema", "table"],
            },
        )
