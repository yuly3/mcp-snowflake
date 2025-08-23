"""
Type definitions for QueryRegistry system.
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import attrs
from snowflake.connector import SnowflakeConnection


class QueryStatus(Enum):
    """Status of a query execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    TIMEOUT = "timeout"


@attrs.define(frozen=True)
class QueryOptions:
    """Options for query execution."""

    query_timeout: timedelta | None = attrs.field(default=None)
    max_inline_rows: int = attrs.field(default=10000)
    poll_interval: float = attrs.field(default=1.0)

    def __attrs_post_init__(self) -> None:
        if self.max_inline_rows <= 0:
            raise ValueError("max_inline_rows must be positive")
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be positive")


@attrs.define(frozen=True)
class ColumnMeta:
    """Column metadata."""

    name: str
    type: str


@attrs.define(frozen=True)
class ResultMeta:
    """Result metadata."""

    row_count: int
    columns: list[ColumnMeta] = attrs.field(factory=list)


@attrs.define(frozen=True)
class ErrorInfo:
    """Error information."""

    type: str
    message: str
    code: int | None = None


@attrs.define(frozen=True)
class SnowflakeInfo:
    """Snowflake-specific information."""

    sfqid: str | None = None


@attrs.define
class QueryRuntime:
    """Internal runtime information (not exposed in snapshots)."""

    sfqid: str | None = None
    connection: SnowflakeConnection | None = None
    poll_interval: float = 1.0
    task: asyncio.Task[Any] | None = None
    canceled: bool = False


@attrs.define
class QueryRecord:
    """Internal mutable query record with lifecycle management methods."""

    query_id: str
    sql: str
    status: QueryStatus
    created_at: datetime
    options: QueryOptions
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result_meta: ResultMeta | None = None
    result_inline: list[dict[str, Any]] | None = None
    error: ErrorInfo | None = None
    ttl_expires_at: datetime | None = None
    cancel_requested: bool = False
    runtime: QueryRuntime | None = attrs.field(default=None, init=False)

    def mark_as_running(
        self,
        *,
        started_at: datetime | None = None,
        sfqid: str | None = None,
        poll_interval: float | None = None,
        task: asyncio.Task[Any] | None = None,
    ) -> None:
        """
        Mark query as running and set runtime information.

        Parameters
        ----------
        started_at : datetime | None, optional
            When the query started running. Uses current UTC time if None.
        sfqid : str | None, optional
            Snowflake query ID
        poll_interval : float | None, optional
            Polling interval for status checks
        task : asyncio.Task | None, optional
            Asyncio task for polling
        """
        from datetime import UTC
        from datetime import datetime as dt

        self.status = QueryStatus.RUNNING
        self.started_at = started_at or dt.now(UTC)

        if self.runtime is None:
            self.runtime = QueryRuntime()

        if sfqid is not None:
            self.runtime.sfqid = sfqid
        if poll_interval is not None:
            self.runtime.poll_interval = poll_interval
        if task is not None:
            self.runtime.task = task

    def mark_as_succeeded(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        columns: list[ColumnMeta] | None = None,
        row_count: int | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        """
        Mark query as successfully completed.

        Parameters
        ----------
        rows : list[dict] | None, optional
            Result rows
        columns : list[ColumnMeta] | None, optional
            Column metadata
        row_count : int | None, optional
            Total row count
        finished_at : datetime | None, optional
            When the query finished. Uses current UTC time if None.
        """
        from datetime import UTC
        from datetime import datetime as dt

        self.status = QueryStatus.SUCCEEDED
        self.finished_at = finished_at or dt.now(UTC)

        if rows is not None:
            self.result_inline = rows

        if columns is not None and row_count is not None:
            self.result_meta = ResultMeta(
                row_count=row_count,
                columns=columns,
            )

    def mark_as_failed(
        self,
        error: Exception | ErrorInfo,
        *,
        finished_at: datetime | None = None,
    ) -> None:
        """
        Mark query as failed.

        Parameters
        ----------
        error : Exception | ErrorInfo
            Error information
        finished_at : datetime | None, optional
            When the query failed. Uses current UTC time if None.
        """
        from datetime import UTC
        from datetime import datetime as dt

        self.status = QueryStatus.FAILED
        self.finished_at = finished_at or dt.now(UTC)

        if isinstance(error, Exception):
            self.error = ErrorInfo(
                type=type(error).__name__,
                message=str(error),
            )
        else:
            self.error = error

    def mark_as_canceled(self, *, finished_at: datetime | None = None) -> None:
        """
        Mark query as canceled.

        Parameters
        ----------
        finished_at : datetime | None, optional
            When the query was canceled. Uses current UTC time if None.
        """
        from datetime import UTC
        from datetime import datetime as dt

        self.status = QueryStatus.CANCELED
        self.finished_at = finished_at or dt.now(UTC)

    def mark_as_timeout(
        self,
        *,
        finished_at: datetime | None = None,
        message: str = "Query execution exceeded timeout limit",
    ) -> None:
        """
        Mark query as timed out.

        Parameters
        ----------
        finished_at : datetime | None, optional
            When the query timed out. Uses current UTC time if None.
        message : str, default="Query execution exceeded timeout limit"
            Timeout error message
        """
        from datetime import UTC
        from datetime import datetime as dt

        self.status = QueryStatus.TIMEOUT
        self.finished_at = finished_at or dt.now(UTC)
        self.error = ErrorInfo(
            type="TimeoutError",
            message=message,
        )

    def request_cancellation(self) -> None:
        """Request query cancellation."""
        self.cancel_requested = True
        if self.runtime is not None:
            self.runtime.canceled = True

    def is_completed(self) -> bool:
        """Check if query is in a completed state."""
        return self.status in [
            QueryStatus.SUCCEEDED,
            QueryStatus.FAILED,
            QueryStatus.CANCELED,
            QueryStatus.TIMEOUT,
        ]

    def is_running(self) -> bool:
        """Check if query is currently running."""
        return self.status == QueryStatus.RUNNING

    def can_be_canceled(self) -> bool:
        """Check if query can be canceled."""
        return not self.is_completed() and not self.cancel_requested

    def get_sfqid(self) -> str | None:
        """Get Snowflake query ID if available."""
        return self.runtime.sfqid if self.runtime else None

    def get_connection(self) -> SnowflakeConnection | None:
        """Get Snowflake connection if available."""
        return self.runtime.connection if self.runtime else None

    def set_connection(self, connection: SnowflakeConnection | None) -> None:
        """Set Snowflake connection."""
        if self.runtime is None:
            self.runtime = QueryRuntime()
        self.runtime.connection = connection


@attrs.define(frozen=True)
class QuerySnapshot:
    """Read-only snapshot of query state for external consumption."""

    query_id: str
    sql: str
    status: QueryStatus
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    result_meta: ResultMeta | None
    error: ErrorInfo | None
    snowflake: SnowflakeInfo


@attrs.define(frozen=True)
class QueryPage:
    """Paginated query result."""

    rows: list[dict[str, Any]]
    total_rows: int
    offset: int
    has_more: bool


def generate_query_id() -> str:
    """Generate a new query ID."""
    return str(uuid.uuid4())
