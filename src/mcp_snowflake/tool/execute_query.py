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

from ..handler import EffectExecuteQuery, ExecuteQueryArgs, handle_execute_query
from .base import Tool


class ExecuteQueryTool(Tool):
    def __init__(
        self,
        json_converter: JsonImmutableConverter,
        effect_handler: EffectExecuteQuery,
    ) -> None:
        self.json_converter = json_converter
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

        try:
            result = await handle_execute_query(
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
        except ValueError as e:
            # For SQL analyzer errors (write operations not allowed)
            text = f"Error: {e}"
        else:
            text = json.dumps(result, indent=2)
        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
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
