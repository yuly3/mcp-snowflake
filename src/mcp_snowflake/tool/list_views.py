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

from ..handler import EffectListViews, ListViewsArgs, handle_list_views
from .base import Tool


class ListViewsTool(Tool):
    def __init__(self, effect_handler: EffectListViews) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "list_views"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = ListViewsArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for list_views: {e}",
                )
            ]

        try:
            result = await handle_list_views(args, self.effect_handler)
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
            description="Retrieve a list of views from a specified database and schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to retrieve views from",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name to retrieve views from",
                    },
                    "filter": {
                        "type": "object",
                        "description": "Optional filter for view names",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["contains"],
                                "description": "Filter type (currently only contains is supported)",
                            },
                            "value": {
                                "type": "string",
                                "description": "Substring to match in view names",
                                "minLength": 1,
                            },
                        },
                        "required": ["type", "value"],
                    },
                },
                "required": ["database", "schema"],
            },
        )
