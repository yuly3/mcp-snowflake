import json
import logging
from typing import Protocol, TypedDict

import mcp.types as types
from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, Schema, Table, TableInfo

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

    database: DataBase
    schema: Schema
    name: str
    column_count: int
    columns: list[ColumnDict]


class TableJsonResponse(TypedDict):
    """TypedDict for the complete table JSON response structure."""

    table_info: TableInfoDict


class DescribeTableArgs(BaseModel):
    database: DataBase
    schema_: Schema = Field(alias="schema")
    table_: Table = Field(alias="table")


class EffectDescribeTable(Protocol):
    async def describe_table(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
    ) -> TableInfo: ...


async def handle_describe_table(
    args: DescribeTableArgs,
    effect_handler: EffectDescribeTable,
) -> list[types.TextContent]:
    """Handle describe_table tool call."""
    try:
        table_data = await effect_handler.describe_table(
            args.database,
            args.schema_,
            args.table_,
        )
    except Exception as e:
        logger.exception("Error describing table")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to describe table: {e!s}",
            )
        ]

    columns_dict: list[ColumnDict] = [
        {
            "name": col.name,
            "data_type": col.data_type.raw_type,
            "nullable": col.nullable,
            "default_value": col.default_value,
            "comment": col.comment,
            "ordinal_position": col.ordinal_position,
        }
        for col in table_data.columns
    ]

    table_json: TableJsonResponse = {
        "table_info": {
            "database": table_data.database,
            "schema": table_data.schema,
            "name": table_data.name,
            "column_count": table_data.column_count,
            "columns": columns_dict,
        }
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(table_json, indent=2),
        )
    ]
