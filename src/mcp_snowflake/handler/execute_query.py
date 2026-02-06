import logging
from collections.abc import Awaitable, Mapping, Sequence
from datetime import timedelta
from typing import Any, Protocol, TypedDict

from more_itertools import first
from pydantic import BaseModel, Field, ValidationInfo, model_validator

from cattrs_converter import Jsonable, JsonImmutableConverter
from kernel import DataProcessingResult

from ..sql_analyzer import SQLWriteDetector
from ..stopwatch import StopWatch

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS_MAX = 300


class QueryResultDict(TypedDict):
    """TypedDict for query result in JSON response."""

    execution_time_ms: int
    row_count: int
    columns: list[str]
    rows: Sequence[Mapping[str, Jsonable]]
    warnings: list[str]


class ExecuteQueryJsonResponse(TypedDict):
    """TypedDict for the complete execute query JSON response structure."""

    query_result: QueryResultDict


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
) -> ExecuteQueryJsonResponse:
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
    ExecuteQueryJsonResponse
        The structured response containing query results.

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
    return ExecuteQueryJsonResponse(
        query_result={
            "execution_time_ms": execution_time_ms,
            "row_count": len(result.processed_rows),
            "columns": columns,
            "rows": result.processed_rows,
            "warnings": result.warnings,
        }
    )
