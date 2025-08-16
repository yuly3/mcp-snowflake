import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Protocol, TypedDict

import mcp.types as types
from pydantic import BaseModel, Field

from cattrs_converter import Jsonable, JsonImmutableConverter
from kernel import DataProcessingResult

logger = logging.getLogger(__name__)


class SampleDataDict(TypedDict):
    """TypedDict for sample data in JSON response."""

    database: str
    schema: str
    table: str
    sample_size: int
    actual_rows: int
    columns: list[str]
    rows: Sequence[Mapping[str, Jsonable]]
    warnings: list[str]


class SampleTableDataJsonResponse(TypedDict):
    """TypedDict for the complete sample table data JSON response structure."""

    sample_data: SampleDataDict


class SampleTableDataArgs(BaseModel):
    database: str
    schema_: str = Field(alias="schema")
    table_: str = Field(alias="table")
    sample_size: int = Field(default=10, ge=1)
    columns: list[str] = Field(default_factory=list)


class EffectSampleTableData(Protocol):
    async def sample_table_data(
        self,
        database: str,
        schema: str,
        table: str,
        sample_size: int,
        columns: list[str],
    ) -> list[dict[str, Any]]: ...


def _format_response(
    processed_rows: Sequence[Mapping[str, Jsonable]],
    warnings: list[str],
    database: str,
    schema: str,
    table: str,
    sample_size: int,
) -> SampleTableDataJsonResponse:
    """
    Format the final response structure.

    Parameters
    ----------
    processed_rows : Sequence[Mapping[str, Jsonable]]
        Processed sample data rows
    warnings : list[str]
        List of warning messages
    database : str
        Database name
    schema : str
        Schema name
    table : str
        Table name
    sample_size : int
        Requested sample size

    Returns
    -------
    SampleTableDataJsonResponse
        Formatted response structure
    """
    return {
        "sample_data": {
            "database": database,
            "schema": schema,
            "table": table,
            "sample_size": sample_size,
            "actual_rows": len(processed_rows),
            "columns": list(processed_rows[0].keys()) if processed_rows else [],
            "rows": processed_rows,
            "warnings": warnings,
        }
    }


async def handle_sample_table_data(
    json_converter: JsonImmutableConverter,
    args: SampleTableDataArgs,
    effect_handler: EffectSampleTableData,
) -> list[types.TextContent]:
    """
    Handle sample_table_data tool call.

    Parameters
    ----------
    json_converter : JsonImmutableConverter
        JSON converter for structuring and unstructuring data
    args : SampleTableDataArgs
        Arguments for the sample table data operation
    effect_handler : EffectSampleTableData
        Handler for Snowflake operations

    Returns
    -------
    list[types.TextContent]
        Response content with sample data or error message
    """
    try:
        raw_data = await effect_handler.sample_table_data(
            args.database,
            args.schema_,
            args.table_,
            args.sample_size,
            args.columns,
        )

    except Exception as e:
        logger.exception("Error handling sample_table_data")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to sample table data: {e}",
            )
        ]

    result = DataProcessingResult.from_raw_rows(json_converter, raw_data)

    response = _format_response(
        result.processed_rows,
        result.warnings,
        args.database,
        args.schema_,
        args.table_,
        args.sample_size,
    )

    return [
        types.TextContent(
            type="text",
            text=json.dumps(response, indent=2, ensure_ascii=False),
        )
    ]
