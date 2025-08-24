"""
QueryRegistry implementation for async query execution management.
"""

import asyncio
import contextlib
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.connector.errors import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

from expression.contract import contract_async

from .connection import SnowflakeConnectionProvider
from .types import (
    ColumnMeta,
    QueryOptions,
    QueryPage,
    QueryRecord,
    QueryRuntime,
    QuerySnapshot,
    QueryStatus,
    SnowflakeInfo,
    generate_query_id,
)

logger = logging.getLogger(__name__)


class QueryRegistry:
    """
    Complete execution management for Snowflake async queries.

    This class provides a comprehensive async query management system that handles
    the full lifecycle of Snowflake queries from execution to result retrieval.

    Features
    --------
    - Asynchronous query execution with polling
    - Query cancellation and timeout handling
    - Result caching and pagination
    - TTL-based automatic cleanup
    - Thread-safe operation with proper resource management

    Typical Workflow
    ---------------
    1. **Execute**: Call execute_query() to start async SQL execution
    2. **Monitor**: Use get_snapshot() to check status and progress
    3. **Retrieve**: Call fetch_result() once completed to get paginated results
    4. **Manage**: Use list_queries() to see all queries, prune_expired() for cleanup

    Thread Safety
    -------------
    All public methods are thread-safe and can be called concurrently from
    multiple asyncio tasks.

    See Also
    --------
    For complete usage patterns and integration examples, refer to:
    `tests/query_registry/test_registry.py`
    """

    def __init__(
        self,
        connection_provider: SnowflakeConnectionProvider,
        executor: ThreadPoolExecutor,
    ) -> None:
        """
        Initialize QueryRegistry.

        Parameters
        ----------
        connection_provider : SnowflakeConnectionProvider
            Snowflake connection provider
        executor : ThreadPoolExecutor
            Thread pool for Snowflake API calls
        """
        self._connection_provider = connection_provider
        self._executor = executor
        self._lock = asyncio.Lock()
        self._store: dict[str, QueryRecord] = {}

    @contract_async(
        known_err=(
            ValueError,
            TypeError,
            DatabaseError,
            OperationalError,
            ProgrammingError,
            InterfaceError,
        )
    )
    async def execute_query(self, sql: str, options: QueryOptions | None = None) -> str:
        """
        Create query and start async execution immediately.

        Parameters
        ----------
        sql : str
            SQL query string to execute
        options : QueryOptions | None, optional
            Query execution options including timeout and polling interval

        Returns
        -------
        str
            Unique query identifier for tracking execution

        Raises
        ------
        ValueError
            If SQL query is empty, invalid, or contains unsupported statements
        TypeError
            If query execution fails to return a valid query ID
        DatabaseError
            Base class for database-related errors from Snowflake
        OperationalError
            Connection failures, network issues, or Snowflake service problems
        ProgrammingError
            SQL syntax errors, invalid queries, or authentication failures
        InterfaceError
            Interface-related errors from the Snowflake connector
        ContractViolationError
            For unexpected internal errors during query setup or execution

        Notes
        -----
        Internal flow:
        1. Create QueryRecord (status=pending)
        2. Execute execute_async() in ThreadPoolExecutor
        3. Get sfqid and save to runtime
        4. Update status=running
        5. Start polling task

        The query will continue executing asynchronously after this method returns.
        Use get_snapshot() to check status and fetch_result() to retrieve results.

        See Also
        --------
        get_snapshot : Check query execution status
        fetch_result : Retrieve query results with pagination
        cancel : Cancel a running query

        For complete usage examples, see tests in:
        `tests/query_registry/test_registry.py::TestQueryRegistry::test_execute_query_integration`
        """
        # Input validation
        if not sql or not sql.strip():
            raise ValueError("SQL query cannot be empty or whitespace-only")

        # 1. Create QueryRecord (pending status)
        query_id = generate_query_id()
        if options is None:
            options = QueryOptions()

        now = datetime.now(UTC)
        record = QueryRecord(
            query_id=query_id,
            sql=sql,
            status=QueryStatus.PENDING,
            created_at=now,
            options=options,
            ttl_expires_at=now + timedelta(hours=1),
        )
        record.runtime = QueryRuntime()

        async with self._lock:
            self._store[query_id] = record

        # 2. Start execution immediately
        try:
            sfqid = await self._execute_async_sync(query_id, sql)

            # 3. Set runtime information and mark as running
            async with self._lock:
                record = self._store[query_id]
                record.mark_as_running(
                    sfqid=sfqid,
                    poll_interval=options.poll_interval,
                )

            # 4. Start polling task
            task = asyncio.create_task(self._poll_until_done(query_id))
            async with self._lock:
                record = self._store[query_id]
                if record.runtime:
                    record.runtime.task = task

        except Exception as e:
            # Execution start failed - cleanup the record
            await self._cleanup_failed_query(query_id, e)
            raise

        return query_id

    @contract_async(known_err=(ValueError, DatabaseError, OperationalError))
    async def cancel(self, query_id: str) -> bool:
        """
        Execute real query cancellation.

        Parameters
        ----------
        query_id : str
            Unique identifier of the query to cancel

        Returns
        -------
        bool
            True if query was successfully cancelled, False if query was not found,
            already completed, or cancellation failed

        Raises
        ------
        ValueError
            If query_id is empty or invalid format
        DatabaseError
            Base class for database-related errors from Snowflake
        OperationalError
            Connection failures or Snowflake service problems during cancellation
        ContractViolationError
            For unexpected internal errors during cancellation process

        Notes
        -----
        Internal flow:
        1. Check if query is already completed (return False if so)
        2. Execute SYSTEM$CANCEL_QUERY(sfqid) with new connection
        3. Stop polling task
        4. Update status=canceled

        Cancellation attempts on non-existent or already completed queries
        will return False without raising an exception.

        See Also
        --------
        execute_query : Start query execution
        get_snapshot : Check if cancellation was successful

        For complete usage examples, see tests in:
        `tests/query_registry/test_registry.py::TestQueryRegistry::test_cancel_integration`
        """
        # Input validation
        if not query_id or not query_id.strip():
            raise ValueError("Query ID cannot be empty or whitespace-only")

        async with self._lock:
            record = self._store.get(query_id)
            if not record or not record.runtime:
                return False

            # Don't cancel already completed queries
            if record.is_completed():
                return False

            sfqid = record.get_sfqid()
            if not sfqid:
                return False

            # Mark cancel requested
            record.request_cancellation()

            # Cancel polling task if exists - extract task for safe cancellation
            task_to_cancel = None
            if (
                record.runtime
                and record.runtime.task
                and not record.runtime.task.done()
            ):
                task_to_cancel = record.runtime.task
                _ = record.runtime.task.cancel()

        # Cancel task outside of lock to avoid blocking
        if task_to_cancel:
            try:
                await task_to_cancel
            except asyncio.CancelledError:
                pass  # Expected when cancelled
            except Exception as e:
                logger.warning(f"Unexpected error during task cancellation: {e}")

        try:
            # Execute cancel query with new connection
            await self._cancel_query_sync(sfqid)

            # Update to canceled status and close connection
            async with self._lock:
                record = self._store[query_id]
                record.mark_as_canceled()
                # Close connection after cancellation
                if record.runtime and record.runtime.connection:
                    await self._close_connection_safely(record.runtime.connection)
                    record.runtime.connection = None
        except Exception as e:
            logger.error(f"Failed to cancel query {query_id}: {e}")
            return False
        return True

    @contract_async(known_err=(ValueError,))
    async def get_snapshot(self, query_id: str) -> QuerySnapshot | None:
        """
        Get current query state as immutable snapshot.

        Parameters
        ----------
        query_id : str
            Unique identifier of the query to retrieve

        Returns
        -------
        QuerySnapshot | None
            Immutable snapshot of query state, or None if query not found

        Raises
        ------
        ValueError
            If query_id is empty or invalid format
        ContractViolationError
            For unexpected internal errors during snapshot retrieval

        Notes
        -----
        Returns a complete snapshot of the query state including:
        - Query ID, SQL, and current status
        - Timestamps (created, started, finished)
        - Result metadata and error information
        - Snowflake-specific information (sfqid)

        This method is safe to call at any time and will not modify query state.

        See Also
        --------
        execute_query : Start query execution
        fetch_result : Retrieve query results
        list_queries : List all queries with optional status filter

        For complete usage examples, see tests in:
        `tests/query_registry/test_registry.py::TestQueryRegistry::test_status_transitions`
        """
        # Input validation
        if not query_id or not query_id.strip():
            raise ValueError("Query ID cannot be empty or whitespace-only")

        async with self._lock:
            record = self._store.get(query_id)
            if not record:
                return None

            snowflake_info = SnowflakeInfo(
                sfqid=record.runtime.sfqid if record.runtime else None
            )

            return QuerySnapshot(
                query_id=record.query_id,
                sql=record.sql,
                status=record.status,
                created_at=record.created_at,
                started_at=record.started_at,
                finished_at=record.finished_at,
                result_meta=record.result_meta,
                error=record.error,
                snowflake=snowflake_info,
            )

    @contract_async(known_err=(ValueError, IndexError))
    async def fetch_result(
        self,
        query_id: str,
        offset: int = 0,
        limit: int | None = None,
    ) -> QueryPage | None:
        """
        Fetch query results with pagination support (inline only).

        Parameters
        ----------
        query_id : str
            Unique identifier of the query to fetch results from
        offset : int, default=0
            Starting position for result pagination
        limit : int | None, default=None
            Maximum number of rows to return, None for no limit

        Returns
        -------
        QueryPage | None
            Paginated result set with rows and metadata, or None if query
            is still running or not found

        Raises
        ------
        ValueError
            If query_id is empty, invalid, or pagination parameters are invalid
        IndexError
            If offset is beyond the available result set
        ContractViolationError
            For unexpected internal errors during result retrieval

        Notes
        -----
        Returns None for:
        - Non-existent queries
        - Currently running queries
        - Queries that haven't started yet

        Returns QueryPage (possibly empty) for:
        - Completed queries (succeeded, failed, canceled)
        - Queries with empty result sets

        The returned QueryPage includes:
        - rows: List of result rows as dictionaries
        - total_rows: Total number of rows available
        - offset: Current starting position
        - has_more: Whether more rows are available

        See Also
        --------
        execute_query : Start query execution
        get_snapshot : Check if query is completed before fetching results

        For complete usage examples, see tests in:
        `tests/query_registry/test_registry.py::TestQueryRegistry::test_fetch_result_pagination`
        """
        # Input validation
        if not query_id or not query_id.strip():
            raise ValueError("Query ID cannot be empty or whitespace-only")
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        if limit is not None and limit < 0:
            raise ValueError("Limit must be positive or None")

        async with self._lock:
            record = self._store.get(query_id)
            if not record:
                return None

            # Return None only for running queries or queries without results yet
            if record.status == QueryStatus.RUNNING or record.result_inline is None:
                return None

            # Always return QueryPage for completed queries, even if empty
            rows = record.result_inline or []  # Handle empty/None results consistently
            total_rows = len(rows)

            if limit is None:
                result_rows = rows[offset:]
                has_more = False
            else:
                end_idx = offset + limit
                result_rows = rows[offset:end_idx]
                has_more = end_idx < total_rows

            return QueryPage(
                rows=result_rows,
                total_rows=total_rows,
                offset=offset,
                has_more=has_more,
            )

    @contract_async()
    async def list_queries(
        self,
        status_filter: QueryStatus | None = None,
    ) -> list[QuerySnapshot]:
        """
        List all queries with optional status filter.

        Parameters
        ----------
        status_filter : QueryStatus | None, default=None
            Optional status to filter queries by. If None, returns all queries.

        Returns
        -------
        list[QuerySnapshot]
            List of immutable query snapshots matching the filter criteria

        Raises
        ------
        ContractViolationError
            For unexpected internal errors during query listing

        Notes
        -----
        Returns all queries in the registry that match the optional status filter.
        Each snapshot contains complete query information including status,
        timestamps, and metadata.

        The returned list is ordered by the internal storage order (not guaranteed
        to be chronological). For time-based ordering, sort by created_at or
        started_at fields.

        See Also
        --------
        get_snapshot : Get details for a specific query
        prune_expired : Remove expired queries from the list

        For complete usage examples, see tests in:
        `tests/query_registry/test_registry.py::TestQueryRegistry::test_list_queries`
        """
        snapshots: list[QuerySnapshot] = []
        async with self._lock:
            for record in self._store.values():
                if status_filter is None or record.status == status_filter:
                    snowflake_info = SnowflakeInfo(
                        sfqid=record.runtime.sfqid if record.runtime else None
                    )
                    snapshots.append(
                        QuerySnapshot(
                            query_id=record.query_id,
                            sql=record.sql,
                            status=record.status,
                            created_at=record.created_at,
                            started_at=record.started_at,
                            finished_at=record.finished_at,
                            result_meta=record.result_meta,
                            error=record.error,
                            snowflake=snowflake_info,
                        )
                    )
        return snapshots

    @contract_async()
    async def prune_expired(self) -> int:
        """
        Remove TTL-expired queries and return count of deleted queries.

        Returns
        -------
        int
            Number of expired queries that were removed

        Raises
        ------
        ContractViolationError
            For unexpected internal errors during cleanup process

        Notes
        -----
        Removes all queries that have exceeded their TTL (time-to-live) expiration.
        This includes:
        - Canceling any running polling tasks for expired queries
        - Removing query records from internal storage
        - Cleaning up associated resources

        This method should be called periodically to prevent memory leaks
        from accumulating old query records.

        See Also
        --------
        list_queries : View all queries before pruning
        close : Complete cleanup when shutting down

        For complete usage examples, see tests in:
        `tests/query_registry/test_registry.py::TestQueryRegistry::test_ttl_edge_cases`
        """
        now = datetime.now(UTC)
        deleted_count = 0
        tasks_to_cancel: list[asyncio.Task[Any]] = []

        async with self._lock:
            expired_ids: list[str] = []
            for query_id, record in self._store.items():
                if record.ttl_expires_at and record.ttl_expires_at <= now:
                    expired_ids.append(query_id)
                    # Collect tasks for cancellation outside the lock
                    if (
                        record.runtime
                        and record.runtime.task
                        and not record.runtime.task.done()
                    ):
                        tasks_to_cancel.append(record.runtime.task)
                        _ = record.runtime.task.cancel()

        # Wait for cancelled tasks outside of lock BEFORE closing connections
        if tasks_to_cancel:
            _ = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        # Safely close connections and remove from store
        async with self._lock:
            for query_id in expired_ids:
                record = self._store.get(query_id)
                if record:
                    # Close connection after tasks are completed
                    if record.runtime and record.runtime.connection:
                        await self._close_connection_safely(record.runtime.connection)
                    del self._store[query_id]
                    deleted_count += 1

        return deleted_count

    @contract_async()
    async def close(self) -> None:
        """
        Clean up all resources and shut down the registry.

        Raises
        ------
        ContractViolationError
            For unexpected internal errors during cleanup process

        Notes
        -----
        Performs complete cleanup including:
        - Canceling all running polling tasks
        - Waiting for tasks to complete gracefully
        - Clearing all query records from memory
        - Releasing associated resources

        This method should be called before the registry is destroyed
        to ensure proper resource cleanup and prevent resource leaks.

        After calling close(), the registry should not be used for
        new operations.

        See Also
        --------
        prune_expired : Clean up expired queries without full shutdown

        For complete usage examples, see tests in:
        `tests/query_registry/test_registry.py::TestQueryRegistry::test_close_cleanup`
        """
        async with self._lock:
            # Cancel all running tasks and collect them for waiting
            tasks_to_wait: list[asyncio.Task[Any]] = []
            for record in self._store.values():
                if (
                    record.runtime
                    and record.runtime.task
                    and not record.runtime.task.done()
                ):
                    _ = record.runtime.task.cancel()
                    tasks_to_wait.append(record.runtime.task)

        # Wait for all tasks to complete outside of lock BEFORE closing connections
        if tasks_to_wait:
            _ = await asyncio.gather(*tasks_to_wait, return_exceptions=True)

        # Safely close connections and clear store
        async with self._lock:
            for record in self._store.values():
                # Close all connections after tasks are completed
                if record.runtime and record.runtime.connection:
                    await self._close_connection_safely(record.runtime.connection)

            self._store.clear()

    async def _execute_async_sync(self, query_id: str, sql: str) -> str:
        """Execute execute_async in sync thread and return sfqid."""
        # Get connection for execution
        conn = self._connection_provider.get_connection()

        # Get timeout from QueryOptions
        async with self._lock:
            record = self._store[query_id]
            timeout = record.options.query_timeout

        sfqid = await asyncio.get_event_loop().run_in_executor(
            self._executor,
            _sync_execute_query,
            conn,
            sql,
            timeout,
        )

        # Store connection for polling
        async with self._lock:
            record = self._store[query_id]
            record.set_connection(conn)

        return sfqid

    async def _poll_until_done(self, query_id: str) -> None:
        """Poll until query execution is done."""
        while True:
            # Check if canceled
            async with self._lock:
                record = self._store.get(query_id)
                if not record or not record.runtime:
                    return
                if record.runtime.canceled:
                    return

                poll_interval = record.runtime.poll_interval
                sfqid = record.get_sfqid()
                conn = record.get_connection()

            if not sfqid or not conn:
                break

            # Check timeout
            if await self._is_timeout_exceeded(query_id):
                await self._handle_timeout(query_id)
                return

            # Poll Snowflake
            try:
                is_running = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    _sync_check_query_status,
                    conn,
                    sfqid,
                )

                if not is_running:
                    break

                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                return
            except Exception as e:
                await self._set_failed(query_id, e)
                return

        # Handle completion
        await self._handle_completion(query_id)

    async def _cancel_query_sync(self, sfqid: str) -> None:
        """Execute SYSTEM$CANCEL_QUERY (using new connection)."""
        # Get new connection for cancellation
        conn = self._connection_provider.get_connection()

        _ = await asyncio.get_event_loop().run_in_executor(
            self._executor,
            _sync_cancel_query,
            conn,
            sfqid,
        )

    async def _is_timeout_exceeded(self, query_id: str) -> bool:
        """Check if query has exceeded timeout."""
        async with self._lock:
            record = self._store.get(query_id)
            if not record:
                return False

            if not record.started_at:
                return False

            elapsed = datetime.now(UTC) - record.started_at
            return elapsed > record.options.query_timeout

    async def _handle_timeout(self, query_id: str) -> None:
        """Handle query timeout."""
        async with self._lock:
            record = self._store.get(query_id)
            if record:
                record.mark_as_timeout()

    async def _handle_completion(self, query_id: str) -> None:
        """Handle query completion."""
        async with self._lock:
            record = self._store.get(query_id)
            if not record or not record.runtime:
                return

            sfqid = record.get_sfqid()
            conn = record.get_connection()
            max_rows = record.options.max_inline_rows

        if sfqid and conn:
            try:
                (
                    rows,
                    columns,
                    row_count,
                ) = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    _sync_get_query_results,
                    conn,
                    sfqid,
                    max_rows,
                )

                async with self._lock:
                    record = self._store[query_id]
                    record.mark_as_succeeded(
                        rows=rows,
                        columns=columns,
                        row_count=row_count,
                    )

            except Exception as e:
                # Set failed status but keep record for debugging
                await self._set_failed(query_id, e)
                logger.error(f"Query {query_id} completion failed: {e}")
            finally:
                # Close connection after getting results
                async with self._lock:
                    # Use get() instead of direct access
                    record = self._store.get(query_id)
                    if record and record.runtime and record.runtime.connection:
                        await self._close_connection_safely(record.runtime.connection)
                        record.runtime.connection = None

    async def _set_failed(self, query_id: str, error: Exception) -> None:
        """Set query to failed status."""
        async with self._lock:
            record = self._store.get(query_id)
            if record:
                record.mark_as_failed(error)

    async def _cleanup_failed_query(self, query_id: str, error: Exception) -> None:
        """Clean up resources for a failed query and remove from store."""
        async with self._lock:
            record = self._store.get(query_id)
            if record:
                # Cancel any running task and wait for completion
                if record.runtime and record.runtime.task:
                    await self._cancel_task_safely(record.runtime.task)

                # Close any connection
                if record.runtime and record.runtime.connection:
                    await self._close_connection_safely(record.runtime.connection)
                    record.runtime.connection = None

                # Remove from store (don't keep failed startup queries)
                del self._store[query_id]

        logger.error(f"Query {query_id} startup failed and cleaned up: {error}")

    async def _cancel_task_safely(self, task: asyncio.Task[Any]) -> None:
        """
        Cancel task safely and wait for completion.

        Parameters
        ----------
        task : asyncio.Task[Any]
            Task to cancel
        """
        if task.done():
            return

        _ = task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # Expected exception when task is cancelled
        except Exception as e:
            logger.warning(f"Task cancellation resulted in unexpected error: {e}")

    async def _close_connection_safely(self, connection: SnowflakeConnection) -> None:
        """
        Close connection safely.

        Parameters
        ----------
        connection : SnowflakeConnection
            Connection to close
        """
        with contextlib.suppress(Exception):
            connection.close()


def _sync_execute_query(
    connection: SnowflakeConnection,
    sql_query: str,
    timeout: timedelta,
) -> str:
    """Execute async query and return sfqid."""
    timeout_seconds = int(timeout.total_seconds())
    try:
        with connection.cursor() as cursor:
            _ = cursor.execute_async(sql_query, timeout=timeout_seconds)
            sfqid = cursor.sfqid
    except Exception:
        # Close connection on error
        connection.close()
        raise

    if sfqid is None:
        msg = "Failed to get query ID from Snowflake"
        raise TypeError(msg)
    return sfqid


def _sync_check_query_status(connection: SnowflakeConnection, sfqid: str) -> bool:
    """Check if query is still running."""
    try:
        status = connection.get_query_status_throw_if_error(sfqid)
        return connection.is_still_running(status)
    except Exception as e:
        logger.error(f"Error checking query status: {e}")
        raise


def _sync_cancel_query(connection: SnowflakeConnection, query_sfqid: str) -> Any:
    """Cancel query using SYSTEM$CANCEL_QUERY."""
    try:
        with connection.cursor() as cursor:
            _ = cursor.execute(f"SELECT SYSTEM$CANCEL_QUERY('{query_sfqid}')")
            return cursor.fetchone()
    finally:
        connection.close()  # Close immediately after cancellation


def _sync_get_query_results(
    connection: SnowflakeConnection,
    sfqid: str,
    max_rows: int,
) -> tuple[list[dict[str, Any]], list[ColumnMeta], int]:
    """Get query results by sfqid."""
    rows: list[dict[str, Any]] = []
    columns: list[ColumnMeta] = []
    count = 0

    try:
        # Use a cursor to get results by sfqid
        # Note: This is a simplified approach - real implementation might vary
        with connection.cursor() as cursor:
            cursor.get_results_from_sfqid(sfqid)

            # Get column metadata
            if cursor.description:
                columns = [
                    ColumnMeta(
                        name=desc.name,
                        type=FIELD_ID_TO_NAME.get(
                            desc.type_code,
                            f"UNKNOWN_TYPE_{desc.type_code}",
                        ),
                    )
                    for desc in cursor.description
                ]

            # Fetch rows up to max_rows
            for row in cursor:
                if count >= max_rows:
                    break
                row_dict = dict(zip([col.name for col in columns], row))
                rows.append(row_dict)
                count += 1
    except Exception as e:
        logger.error(f"Error getting results: {e}")
        raise
    return rows, columns, count
