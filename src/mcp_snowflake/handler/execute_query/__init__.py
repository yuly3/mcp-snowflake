"""Execute query handler module."""

import logging
from collections.abc import Awaitable
from datetime import timedelta
from typing import Any, Protocol

from more_itertools import first
from pydantic import BaseModel, Field, ValidationInfo, model_validator

from cattrs_converter import JsonImmutableConverter
from kernel import DataProcessingResult

from ...sql_analyzer import SQLWriteDetector
from ...stopwatch import StopWatch
from ._serializer import (
    CompactQueryResultSerializer,
    JsonQueryResultSerializer,
    QueryResult,
    QueryResultSerializer,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS_MAX = 300

# Public API exports
__all__ = [
    "CompactQueryResultSerializer",
    "EffectExecuteQuery",
    "ExecuteQueryArgs",
    "JsonQueryResultSerializer",
    "QueryResult",
    "QueryResultSerializer",
    "handle_execute_query",
]


class ExecuteQueryArgs(BaseModel):
    sql: str
    timeout_seconds: int = Field(default=DEFAULT_TIMEOUT_SECONDS, ge=1)

    @model_validator(mode="after")
    def validate_timeout_seconds_max(self, info: ValidationInfo) -> "ExecuteQueryArgs":
        max_timeout = DEFAULT_TIMEOUT_SECONDS_MAX
        if isinstance(info.context, dict) and "timeout_seconds_max" in info.context:
            max_timeout = int(info.context["timeout_seconds_max"])

        if self.timeout_seconds > max_timeout:
            msg = f"timeout_seconds must be less than or equal to {max_timeout}"
            raise ValueError(msg)
        return self


class EffectExecuteQuery(Protocol):
    def execute_query(
        self,
        query: str,
        query_timeout: timedelta | None = None,
    ) -> Awaitable[list[dict[str, Any]]]:
        """Execute SQL query operation.

        Parameters
        ----------
        query : str
            SQL query to execute (read operations only).
        query_timeout : timedelta | None
            Query timeout duration, by default None.

        Returns
        -------
        Awaitable[list[dict[str, Any]]]
            Raw query result rows from Snowflake.

        Raises
        ------
        TimeoutError
            If query execution times out
        ProgrammingError
            SQL syntax errors or other programming errors
        OperationalError
            Database operation related errors
        DataError
            Data processing related errors
        IntegrityError
            Referential integrity constraint violations
        NotSupportedError
            When an unsupported database feature is used
        """
        ...


async def handle_execute_query(
    json_converter: JsonImmutableConverter,
    args: ExecuteQueryArgs,
    effect_handler: EffectExecuteQuery,
) -> QueryResult:
    """Handle execute_query tool call.

    Parameters
    ----------
    json_converter : JsonImmutableConverter
        JSON converter for structuring and unstructuring data.
    args : ExecuteQueryArgs
        Arguments for the query execution.
    effect_handler : EffectExecuteQuery
        Handler for Snowflake operations.

    Returns
    -------
    QueryResult
        Format-agnostic query result, ready for serialization.

    Raises
    ------
    TimeoutError
        If query execution times out
    ProgrammingError
        SQL syntax errors or other programming errors
    OperationalError
        Database operation related errors
    DataError
        Data processing related errors
    IntegrityError
        Referential integrity constraint violations
    NotSupportedError
        When an unsupported database feature is used
    """
    # SQL safety check
    detector = SQLWriteDetector()
    if detector.is_write_sql(args.sql):
        msg = "Write operations are not allowed. Only read operations (SELECT, SHOW, DESCRIBE, etc.) are permitted."
        raise ValueError(msg)

    stopwatch = StopWatch.start()

    raw_data = await effect_handler.execute_query(
        args.sql,
        timedelta(seconds=args.timeout_seconds),
    )

    execution_time_ms = int(stopwatch.elapsed_ms())

    result = DataProcessingResult.from_raw_rows(json_converter, raw_data)

    columns = list(first(result.processed_rows, {}).keys())
    return QueryResult(
        execution_time_ms=execution_time_ms,
        columns=columns,
        rows=result.processed_rows,
        warnings=result.warnings,
    )
