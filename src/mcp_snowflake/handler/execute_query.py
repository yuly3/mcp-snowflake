import json
import logging
import time
from datetime import timedelta
from typing import Any, Protocol, TypedDict

import mcp.types as types
from pydantic import BaseModel

from ..sql_analyzer import SQLWriteDetector
from .data_processing import process_multiple_rows_data

logger = logging.getLogger(__name__)


class QueryResultDict(TypedDict):
    """TypedDict for query result in JSON response."""

    sql: str
    execution_time_ms: int
    row_count: int
    columns: list[str]
    rows: list[dict[str, Any]]
    warnings: list[str]


class ExecuteQueryJsonResponse(TypedDict):
    """TypedDict for the complete execute query JSON response structure."""

    query_result: QueryResultDict


class ExecuteQueryArgs(BaseModel):
    sql: str
    timeout_seconds: int = 30


class EffectExecuteQuery(Protocol):
    async def execute_query(
        self,
        query: str,
        query_timeout: Any,  # timedelta
    ) -> list[dict[str, Any]]: ...


def _format_query_response(
    processed_rows: list[dict[str, Any]],
    warnings: list[str],
    sql: str,
    execution_time_ms: int,
) -> ExecuteQueryJsonResponse:
    """
    Format the final query response structure.

    Parameters
    ----------
    processed_rows : list[dict[str, Any]]
        Processed query result rows
    warnings : list[str]
        List of warning messages
    sql : str
        Executed SQL query
    execution_time_ms : int
        Query execution time in milliseconds

    Returns
    -------
    ExecuteQueryJsonResponse
        Formatted response structure
    """
    response: ExecuteQueryJsonResponse = {
        "query_result": {
            "sql": sql,
            "execution_time_ms": execution_time_ms,
            "row_count": len(processed_rows),
            "columns": list(processed_rows[0].keys()) if processed_rows else [],
            "rows": processed_rows,
            "warnings": warnings,
        }
    }

    return response


async def handle_execute_query(
    args: ExecuteQueryArgs,
    effect_handler: EffectExecuteQuery,
) -> list[types.TextContent]:
    """
    Handle execute_query tool call.

    Parameters
    ----------
    args : ExecuteQueryArgs
        Arguments for the query execution
    effect_handler : EffectExecuteQuery
        Handler for Snowflake operations

    Returns
    -------
    list[types.TextContent]
        Response content with query results or error message
    """
    # SQL安全性チェック
    detector = SQLWriteDetector()
    try:
        if detector.is_write_sql(args.sql):
            return [
                types.TextContent(
                    type="text",
                    text="Error: Write operations are not allowed. Only read operations (SELECT, SHOW, DESCRIBE, etc.) are permitted.",
                )
            ]
    except Exception as e:
        logger.exception("Error analyzing SQL")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to analyze SQL: {e}",
            )
        ]

    try:
        start_time = time.perf_counter()
        raw_data = await effect_handler.execute_query(
            args.sql,
            timedelta(seconds=args.timeout_seconds),
        )
        end_time = time.perf_counter()

    except Exception as e:
        logger.exception("Error executing query")
        return [
            types.TextContent(
                type="text",
                text=f"Error: Failed to execute query: {e}",
            )
        ]

    execution_time_ms = int((end_time - start_time) * 1000)

    result = process_multiple_rows_data(raw_data)

    response = _format_query_response(
        result["processed_rows"],
        result["warnings"],
        args.sql,
        execution_time_ms,
    )

    return [
        types.TextContent(
            type="text",
            text=json.dumps(response, indent=2, ensure_ascii=False),
        )
    ]
