import json
import logging
from typing import Any, Protocol, TypedDict

import mcp.types as types
from pydantic import BaseModel

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


def _check_json_serializable(value: Any) -> tuple[bool, str | None]:
    """
    Check if a value is JSON serializable.

    Parameters
    ----------
    value : Any
        The value to check for JSON serializability

    Returns
    -------
    tuple[bool, str | None]
        Tuple containing (is_serializable, type_name_if_not_serializable)
    """
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return False, str(type(value).__name__)
    else:
        return True, None


def _process_sample_data(
    raw_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Convert raw data to JSON-compatible format and generate warnings.

    Parameters
    ----------
    raw_rows : list[dict[str, Any]]
        Raw sample data from Snowflake

    Returns
    -------
    tuple[list[dict[str, Any]], list[str]]
        Tuple containing (processed_rows, warnings_list)
    """
    if not raw_rows:
        return [], []

    processed_rows = []
    warnings_set = set()

    for row in raw_rows:
        processed_row = {}
        for column, value in row.items():
            is_serializable, type_name = _check_json_serializable(value)
            if is_serializable:
                processed_row[column] = value
            else:
                processed_row[column] = f"<unsupported_type: {type_name}>"
                warnings_set.add(f"対応していない型の列 '{column}' が含まれています")
        processed_rows.append(processed_row)

    return processed_rows, list(warnings_set)


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
        # SnowflakeClientから生データを取得
        raw_data = await effect_handler.sample_table_data(
            args.database,
            args.schema_name,
            args.table_name,
            args.sample_size,
            args.columns,
        )

        # JSON処理とwarnings生成
        processed_data, warnings = _process_sample_data(raw_data)

        # レスポンス形式に整形
        response = _format_response(
            processed_data,
            warnings,
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
