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
    EffectProfileSemiStructuredColumns,
    ProfileSemiStructuredColumnsArgs,
    handle_profile_semi_structured_columns,
)
from ..handler.profile_semi_structured_columns import (
    CompactProfileSemiStructuredColumnsResultSerializer,
    NoSemiStructuredColumns,
    SemiStructuredColumnDoesNotExist,
    SemiStructuredProfileResultParseError,
)
from .base import Tool


class ProfileSemiStructuredColumnsTool(Tool):
    def __init__(
        self,
        effect_handler: EffectProfileSemiStructuredColumns,
    ) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "profile_semi_structured_columns"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = ProfileSemiStructuredColumnsArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for profile_semi_structured_columns: {e}",
                )
            ]

        try:
            result = await handle_profile_semi_structured_columns(args, self.effect_handler)
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
        except SemiStructuredProfileResultParseError as e:
            text = f"Error: Snowflake returned unexpected result format: {e}"
        except ContractViolationError as e:
            text = f"Error: Unexpected error: {e}"
        else:
            match result:
                case SemiStructuredColumnDoesNotExist(not_existed_columns=not_existed_columns):
                    text = f"Error: Columns not found in table: {', '.join(not_existed_columns)}"
                case NoSemiStructuredColumns(unsupported_columns=unsupported_columns):
                    unsupported_list = [f"{col.name}({col.data_type.raw_type})" for col in unsupported_columns]
                    text = "Error: No semi-structured columns to profile. " + (
                        f"Unsupported columns: {', '.join(unsupported_list)}"
                    )
                case response:
                    text = response.serialize_with(CompactProfileSemiStructuredColumnsResultSerializer())

        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Profile semi-structured columns (VARIANT/ARRAY/OBJECT) with sampled flatten-based analysis. "
            + "Returns column-level null/type/array stats and optional path-level distributions.",
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
                        "description": "Name of the table to profile",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": [],
                        "description": "Columns to profile (empty selects all VARIANT/ARRAY/OBJECT columns)",
                    },
                    "sample_rows": {
                        "type": "integer",
                        "default": 10000,
                        "minimum": 1,
                        "maximum": 200000,
                        "description": "Sample size in rows for approximate profiling",
                    },
                    "max_depth": {
                        "type": "integer",
                        "default": 4,
                        "minimum": 1,
                        "maximum": 20,
                        "description": "Maximum recursive path depth for flatten-based path profiling",
                    },
                    "top_k_limit": {
                        "type": "integer",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 100,
                        "description": "Top-k limit for frequent values and keys",
                    },
                    "include_path_stats": {
                        "type": "boolean",
                        "default": True,
                        "description": "Whether to include path-level profiling output",
                    },
                    "include_value_samples": {
                        "type": "boolean",
                        "default": False,
                        "description": "Whether to include top_values samples in path-level output",
                    },
                },
                "required": ["database", "schema", "table"],
            },
        )
