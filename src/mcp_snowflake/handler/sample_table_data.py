import json
import logging
from typing import Any, Protocol, TypedDict

import mcp.types as types
from pydantic import BaseModel

from .data_processing import process_multiple_rows_data

logger = logging.getLogger(__name__)


class SampleDataDict(TypedDict):
    """TypedDict for sample data in JSON response."""

    database: str
    schema: str
    table: str
    sample_size: int
    actual_rows: int
    columns: list[str]
    rows: list[dict[str, Any]]
    warnings: list[str]


class SampleTableDataJsonResponse(TypedDict):
    """TypedDict for the complete sample table data JSON response structure."""

    sample_data: SampleDataDict


class SampleTableDataArgs(BaseModel):
    database: str
    schema_name: str
    table_name: str
    sample_size: int = 10
    columns: list[str] | None = None


class EffectSampleTableData(Protocol):
    async def sample_table_data(
        self,
        database: str,
        schema: str,
        table_name: str,
        sample_size: int,
        columns: list[str] | None,
    ) -> list[dict[str, Any]]: ...


def _format_response(
    processed_rows: list[dict[str, Any]],
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
    processed_rows : list[dict[str, Any]]
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
    response: SampleTableDataJsonResponse = {
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

    return response


async def handle_sample_table_data(
    args: SampleTableDataArgs,
    effect_handler: EffectSampleTableData,
) -> list[types.TextContent]:
    """
    Handle sample_table_data tool call.

    Parameters
    ----------
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
            args.schema_name,
            args.table_name,
            args.sample_size,
            args.columns,
        )

        result = process_multiple_rows_data(raw_data)

        response = _format_response(
            result["processed_rows"],
            result["warnings"],
            args.database,
            args.schema_name,
            args.table_name,
            args.sample_size,
        )

        return [
            types.TextContent(
                type="text",
                text=json.dumps(response, indent=2, ensure_ascii=False),
            )
        ]

    except Exception as e:
        logger.exception("Error handling sample_table_data")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to sample table data: {e}",
            )
        ]
