import json
import logging
from typing import Any, Protocol, TypedDict

import mcp.types as types
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ColumnDict(TypedDict):
    """TypedDict for column information in JSON response."""

    name: str
    data_type: str
    nullable: bool
    default_value: str | None
    comment: str | None
    ordinal_position: int


class TableInfoDict(TypedDict):
    """TypedDict for table information in JSON response."""

    database: str
    schema: str
    name: str
    column_count: int
    columns: list[ColumnDict]


class TableJsonResponse(TypedDict):
    """TypedDict for the complete table JSON response structure."""

    table_info: TableInfoDict


class DescribeTableArgs(BaseModel):
    database: str
    schema_name: str
    table_name: str


class TableColumn(BaseModel):
    name: str
    data_type: str
    nullable: bool
    default_value: str | None
    comment: str | None
    ordinal_position: int


class TableInfo(BaseModel):
    database: str
    schema_name: str
    name: str
    column_count: int
    columns: list[TableColumn]


class EffectDescribeTable(Protocol):
    async def describe_table(
        self,
        database: str,
        schema: str,
        table_name: str,
    ) -> dict[str, Any]: ...


async def handle_describe_table(
    args: DescribeTableArgs,
    effect_handler: EffectDescribeTable,
) -> list[types.TextContent]:
    """Handle describe_table tool call."""
    try:
        table_data = await effect_handler.describe_table(
            args.database,
            args.schema_name,
            args.table_name,
        )
    except Exception as e:
        logger.exception("Error describing table")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to describe table: {e!s}",
            )
        ]

    columns = [
        TableColumn(
            name=col["name"],
            data_type=col["data_type"],
            nullable=col["nullable"],
            default_value=col["default_value"],
            comment=col["comment"],
            ordinal_position=col["ordinal_position"],
        )
        for col in table_data["columns"]
    ]

    table_info = TableInfo(
        database=table_data["database"],
        schema_name=table_data["schema_name"],
        name=table_data["name"],
        column_count=table_data["column_count"],
        columns=columns,
    )

    columns_dict: list[ColumnDict] = [
        {
            "name": col.name,
            "data_type": col.data_type,
            "nullable": col.nullable,
            "default_value": col.default_value,
            "comment": col.comment,
            "ordinal_position": col.ordinal_position,
        }
        for col in table_info.columns
    ]

    table_json: TableJsonResponse = {
        "table_info": {
            "database": table_info.database,
            "schema": table_info.schema_name,
            "name": table_info.name,
            "column_count": table_info.column_count,
            "columns": columns_dict,
        }
    }

    required_fields = [col.name for col in table_info.columns if not col.nullable]
    optional_fields = [col.name for col in table_info.columns if col.nullable]

    # For primary key detection, we'll use a simple heuristic for now
    # (first non-nullable column with "ID" in name or first column)
    primary_key_candidates = [
        col.name
        for col in table_info.columns
        if not col.nullable and ("id" in col.name.lower() or col.ordinal_position == 1)
    ]
    primary_key = (
        primary_key_candidates[0] if primary_key_candidates else "Not identified"
    )

    # Format the hybrid response
    json_str = json.dumps(table_json, indent=2)

    response_text = f"""{json_str}

**Key characteristics:**
- Primary key: {primary_key}
- Required fields: {", ".join(required_fields) if required_fields else "None"}
- Optional fields: {", ".join(optional_fields) if optional_fields else "None"}"""

    return [
        types.TextContent(
            type="text",
            text=response_text,
        )
    ]
