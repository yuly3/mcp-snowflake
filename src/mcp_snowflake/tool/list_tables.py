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
    CompactListTablesResultSerializer,
    EffectListTables,
    ListTablesArgs,
    MissingResponseColumnError,
    handle_list_tables,
)
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

        try:
            result = await handle_list_tables(args, self.effect_handler)
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
            text = result.serialize_with(CompactListTablesResultSerializer())
        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Retrieve a list of tables and views from a specified database and schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to retrieve objects from",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name to retrieve objects from",
                    },
                    "filter": {
                        "type": "object",
                        "description": "Optional filter for results. Use type='contains' to filter by name substring, or type='object_type' to filter by TABLE or VIEW.",
                        "oneOf": [
                            {
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["contains"],
                                        "description": "Filter by name substring (case-insensitive)",
                                    },
                                    "value": {
                                        "type": "string",
                                        "description": "Substring to match in object names",
                                        "minLength": 1,
                                    },
                                },
                                "required": ["type", "value"],
                            },
                            {
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["object_type"],
                                        "description": "Filter by object type",
                                    },
                                    "value": {
                                        "type": "string",
                                        "enum": ["TABLE", "VIEW"],
                                        "description": "Object type to filter by",
                                    },
                                },
                                "required": ["type", "value"],
                            },
                        ],
                    },
                },
                "required": ["database", "schema"],
            },
        )
