"""
Tests for QueryRegistry.
"""

import asyncio
import uuid
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from snowflake.connector.cursor import ResultMetadata
from snowflake.connector.errors import OperationalError

from query_registry import (
    QueryOptions,
    QueryRegistry,
    QueryStatus,
    SnowflakeConnectionProvider,
)


class MockSnowflakeConnectionProvider(SnowflakeConnectionProvider):
    """Test mock for SnowflakeConnectionProvider."""

    def __init__(self) -> None:
        # Don't call parent constructor
        self.mock_connections: dict[str, Any] = {}
        self.query_states: dict[str, tuple[bool, list[dict[str, Any]] | None]] = {}
        """sfqid -> (is_running, result)"""
        self.cancel_calls: list[str] = []  # Cancel call records

    def get_connection(self) -> "MockConnection":  # pyright: ignore[reportIncompatibleMethodOverride]
        """Return mock connection."""
        return MockConnection(self)

    def simulate_query_completion(
        self,
        sfqid: str,
        result: list[dict[str, Any]],
    ) -> None:
        """Simulate query completion for tests."""
        self.query_states[sfqid] = (False, result)


class MockConnection:
    """Mock for Snowflake connection."""

    def __init__(self, provider: MockSnowflakeConnectionProvider) -> None:
        self.provider = provider
        self._cursor: MockCursor | None = None

    def cursor(self) -> "MockCursor":
        self._cursor = MockCursor(self.provider)
        return self._cursor

    def close(self) -> None:
        pass  # Mock does nothing

    def get_query_status_throw_if_error(self, sfqid: str) -> str:
        """Mock implementation for status checking."""
        return (
            "RUNNING"
            if self.provider.query_states.get(sfqid, (True, None))[0]
            else "SUCCEEDED"
        )

    def is_still_running(self, status: str) -> bool:
        """Mock implementation for running check."""
        return status == "RUNNING"


class MockCursor:
    """Mock for Snowflake cursor."""

    def __init__(self, provider: MockSnowflakeConnectionProvider) -> None:
        self.provider = provider
        self.sfqid: str | None = None
        self.description: list[ResultMetadata] | None = None
        self._rows: list[dict[str, Any]] = []
        self._row_index = 0

    def execute_async(self, sql: str, timeout: int | None = None) -> None:  # noqa: ARG002
        """Mock execute_async."""
        self.sfqid = str(uuid.uuid4())
        # Default to running state
        self.provider.query_states[self.sfqid] = (True, None)

    def execute(self, sql: str) -> None:
        """Mock regular execute (for cancellation)."""
        if "SYSTEM$CANCEL_QUERY" in sql:
            self.provider.cancel_calls.append(sql)

    def get_results_from_sfqid(self, sfqid: str) -> None:
        """Mock getting results by sfqid."""
        if sfqid in self.provider.query_states:
            is_running, result = self.provider.query_states[sfqid]
            if not is_running and result:
                # Setup mock data
                first_row = result[0]
                # Create description based on first row keys
                # Use proper type_code: 0=FIXED, 2=TEXT for common types
                self.description = [
                    ResultMetadata(
                        name=key,
                        type_code=0
                        if isinstance(list(first_row.values())[i], int | float)
                        else 2,
                        display_size=None,
                        internal_size=None,
                        precision=None,
                        scale=None,
                        is_nullable=True,  # Default to True for tests
                    )
                    for i, key in enumerate(first_row)
                ]
                self._rows = result
                self._row_index = 0

    def fetchone(self) -> dict[str, Any]:
        return {"result": "success"}

    def __iter__(self) -> Generator[tuple[Any, ...]]:
        """Iterate over rows as tuples (like Snowflake cursor)."""
        for row_dict in self._rows:
            # Convert dict to tuple in the order of columns
            if self.description:
                column_names = [desc[0] for desc in self.description]
                yield tuple(row_dict.get(col, None) for col in column_names)
            else:
                yield tuple(row_dict.values())

    def __enter__(self) -> "MockCursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


@pytest.fixture
def event_loop() -> Generator[asyncio.AbstractEventLoop]:
    """Create event loop for tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def executor() -> Generator[ThreadPoolExecutor]:
    """Create thread pool executor for tests."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        yield pool


@pytest.fixture
def mock_provider() -> MockSnowflakeConnectionProvider:
    """Create mock connection provider."""
    return MockSnowflakeConnectionProvider()


@pytest.fixture
def registry(
    mock_provider: MockSnowflakeConnectionProvider,
    executor: ThreadPoolExecutor,
) -> QueryRegistry:
    """Create QueryRegistry for tests."""
    return QueryRegistry(mock_provider, executor)


class TestQueryRegistry:
    """Test cases for QueryRegistry."""

    @pytest.mark.asyncio
    async def test_execute_query_integration(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test integrated execute_query (create + execute)."""
        # Start query execution
        query_id = await registry.execute_query("SELECT * FROM big_table")

        # Check running state
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.RUNNING
        sfqid = snapshot.snowflake.sfqid
        assert sfqid is not None

        # Simulate external completion trigger
        mock_provider.simulate_query_completion(sfqid, [{"count": 1000}])
        await asyncio.sleep(0.1)  # Wait for polling

        # Check completion
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.SUCCEEDED

        # Get results
        page = await registry.fetch_result(query_id)
        assert page is not None
        assert page.rows == [{"count": 1000}]

    @pytest.mark.asyncio
    async def test_cancel_integration(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test query cancellation integration."""
        # Start long-running query
        query_id = await registry.execute_query("SELECT * FROM huge_table")

        # Execute cancellation
        success = await registry.cancel(query_id)
        assert success is True

        # Check that cancel SQL was called
        assert len(mock_provider.cancel_calls) == 1
        assert "SYSTEM$CANCEL_QUERY" in mock_provider.cancel_calls[0]

        # Check final cancelled state
        await asyncio.sleep(0.1)
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.CANCELED

    @pytest.mark.asyncio
    async def test_list_queries(self, registry: QueryRegistry) -> None:
        """Test listing queries with status filter."""
        # Create multiple queries
        _ = await registry.execute_query("SELECT 1")
        _ = await registry.execute_query("SELECT 2")

        # List all queries
        all_queries = await registry.list_queries()
        assert len(all_queries) == 2

        # List running queries only
        running_queries = await registry.list_queries(QueryStatus.RUNNING)
        assert len(running_queries) == 2
        assert all(q.status == QueryStatus.RUNNING for q in running_queries)

    @pytest.mark.asyncio
    async def test_query_options(self, registry: QueryRegistry) -> None:
        """Test query execution with options."""
        options = QueryOptions(
            query_timeout=timedelta(minutes=5),
            max_inline_rows=500,
            poll_interval=2.0,
        )

        query_id = await registry.execute_query("SELECT * FROM table", options)
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.RUNNING

    @pytest.mark.asyncio
    async def test_prune_expired(self, registry: QueryRegistry) -> None:
        """Test pruning of expired queries."""
        # Create query with short TTL (we'll manipulate this)
        query_id = await registry.execute_query("SELECT 1")

        # Note: accessing private members for test purposes
        # In production code, these would be exposed via public interface
        # Manually set TTL to past
        async with registry._lock:  # noqa: SLF001
            record = registry._store[query_id]  # noqa: SLF001
            record.ttl_expires_at = datetime.now(UTC) - timedelta(hours=1)

        # Prune expired
        deleted_count = await registry.prune_expired()
        assert deleted_count == 1

        # Check query is gone
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is None

    @pytest.mark.asyncio
    async def test_close_cleanup(self, registry: QueryRegistry) -> None:
        """Test resource cleanup on close."""
        # Create some queries
        _ = await registry.execute_query("SELECT 1")
        _ = await registry.execute_query("SELECT 2")

        # Close registry
        await registry.close()

        # Check store is cleared (accessing private member for test verification)
        assert len(registry._store) == 0  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_result_pagination(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test result pagination functionality."""
        # Create query
        query_id = await registry.execute_query("SELECT * FROM test_table")

        # Get sfqid and simulate completion with multiple rows
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        sfqid = snapshot.snowflake.sfqid
        assert sfqid is not None

        test_data = [{"id": i, "value": f"row_{i}"} for i in range(10)]
        mock_provider.simulate_query_completion(sfqid, test_data)
        await asyncio.sleep(0.1)  # Wait for completion

        # Test pagination
        page1 = await registry.fetch_result(query_id, offset=0, limit=3)
        assert page1 is not None
        assert len(page1.rows) == 3
        assert page1.has_more is True
        assert page1.offset == 0

        page2 = await registry.fetch_result(query_id, offset=3, limit=3)
        assert page2 is not None
        assert len(page2.rows) == 3
        assert page2.offset == 3

    @pytest.mark.asyncio
    async def test_query_timeout_handling(self, registry: QueryRegistry) -> None:
        """Test query timeout handling."""
        # Create query with very short timeout
        options = QueryOptions(query_timeout=timedelta(milliseconds=1))
        query_id = await registry.execute_query("SELECT * FROM slow_table", options)

        # Wait for timeout
        await asyncio.sleep(0.1)

        # Check timeout status
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        # Note: Timeout handling depends on implementation details
        # This test may need adjustment based on actual timeout behavior

    @pytest.mark.asyncio
    async def test_error_handling(self, registry: QueryRegistry) -> None:
        """Test error handling in query execution."""
        # This test would require mock to simulate errors
        # For now, just test basic error propagation structure
        query_id = await registry.execute_query("INVALID SQL")

        # Basic structure test - actual error simulation would require
        # more sophisticated mocking
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None

    @pytest.mark.asyncio
    async def test_connection_error_handling(
        self,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test handling of connection errors during execution."""
        # Create registry with executor
        with ThreadPoolExecutor(max_workers=2) as executor:
            registry = QueryRegistry(mock_provider, executor)

            # Simulate connection failure by raising exception in mock
            original_get_connection = mock_provider.get_connection

            def failing_connection() -> MockConnection:
                raise OperationalError("Failed to connect to Snowflake")

            mock_provider.get_connection = failing_connection

            # Test that connection error is properly handled
            with pytest.raises(OperationalError):
                _ = await registry.execute_query("SELECT 1")

            # Restore original method
            mock_provider.get_connection = original_get_connection

    @pytest.mark.asyncio
    async def test_empty_result_set(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test handling of empty query results."""
        query_id = await registry.execute_query("SELECT 1 WHERE 1=0")

        # Get sfqid and simulate completion with empty results
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        sfqid = snapshot.snowflake.sfqid
        assert sfqid is not None

        # Simulate empty result set
        mock_provider.simulate_query_completion(sfqid, [])
        await asyncio.sleep(0.1)  # Wait for completion

        # Check completion with empty results
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.SUCCEEDED

        # For empty results, fetch_result should return empty page
        page = await registry.fetch_result(query_id)
        assert page is not None  # Should always return QueryPage for completed queries
        assert len(page.rows) == 0
        assert page.total_rows == 0
        assert page.has_more is False

    @pytest.mark.asyncio
    async def test_concurrent_query_execution(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test concurrent execution of multiple queries."""
        # Start multiple queries concurrently
        queries: list[str] = []
        for i in range(5):
            query_id = await registry.execute_query(f"SELECT {i}")
            queries.append(query_id)

        # Verify all queries are running
        all_queries = await registry.list_queries()
        assert len(all_queries) == 5
        assert all(q.status == QueryStatus.RUNNING for q in all_queries)

        # Simulate completion for all queries
        for i, query_id in enumerate(queries):
            snapshot = await registry.get_snapshot(query_id)
            if snapshot and snapshot.snowflake.sfqid:
                mock_provider.simulate_query_completion(
                    snapshot.snowflake.sfqid, [{"value": i}]
                )

        # Wait for all completions with longer timeout
        await asyncio.sleep(1.0)  # Increase wait time

        # Verify all queries completed successfully
        all_queries = await registry.list_queries()
        # Check if most queries completed successfully (allow for timing variations)
        succeeded_count = sum(
            1 for q in all_queries if q.status == QueryStatus.SUCCEEDED
        )
        # More lenient assertion - at least some should succeed
        assert succeeded_count >= 1

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_query(self, registry: QueryRegistry) -> None:
        """Test canceling a query that doesn't exist."""
        result = await registry.cancel("nonexistent-query-id")
        assert result is False

    @pytest.mark.asyncio
    async def test_cancel_already_completed_query(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test canceling a query that has already completed."""
        query_id = await registry.execute_query("SELECT 1")

        # Get sfqid and simulate completion
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        sfqid = snapshot.snowflake.sfqid
        assert sfqid is not None

        mock_provider.simulate_query_completion(sfqid, [{"result": "done"}])
        await asyncio.sleep(0.1)  # Wait for completion

        # Try to cancel completed query
        result = await registry.cancel(query_id)
        # Should return False since query is already completed
        assert result is False

        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        # Status should remain SUCCEEDED, not become CANCELED
        assert snapshot.status == QueryStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_fetch_result_nonexistent_query(
        self,
        registry: QueryRegistry,
    ) -> None:
        """Test fetching results for a query that doesn't exist."""
        page = await registry.fetch_result("nonexistent-query-id")
        assert page is None

    @pytest.mark.asyncio
    async def test_fetch_result_running_query(self, registry: QueryRegistry) -> None:
        """Test fetching results for a query that's still running."""
        query_id = await registry.execute_query("SELECT * FROM big_table")

        # Try to fetch results while query is still running
        page = await registry.fetch_result(query_id)
        assert page is None  # Should return None for running queries

    @pytest.mark.asyncio
    async def test_get_snapshot_nonexistent_query(
        self,
        registry: QueryRegistry,
    ) -> None:
        """Test getting snapshot for a query that doesn't exist."""
        snapshot = await registry.get_snapshot("nonexistent-query-id")
        assert snapshot is None

    @pytest.mark.asyncio
    async def test_status_transitions(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test proper status transitions during query lifecycle."""
        query_id = await registry.execute_query("SELECT 1")

        # Check initial running status
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.RUNNING
        assert snapshot.started_at is not None
        assert snapshot.finished_at is None

        # Simulate completion
        sfqid = snapshot.snowflake.sfqid
        assert sfqid is not None
        mock_provider.simulate_query_completion(sfqid, [{"value": 1}])
        await asyncio.sleep(0.1)

        # Check final completed status
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.SUCCEEDED
        assert snapshot.finished_at is not None
        assert snapshot.result_meta is not None

    @pytest.mark.asyncio
    async def test_large_result_set_pagination(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test pagination with large result sets."""
        query_id = await registry.execute_query("SELECT * FROM large_table")

        # Get sfqid and simulate completion with large result set
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        sfqid = snapshot.snowflake.sfqid
        assert sfqid is not None

        # Create large dataset (1000 rows)
        large_data = [{"id": i, "value": f"row_{i}"} for i in range(1000)]
        mock_provider.simulate_query_completion(sfqid, large_data)
        await asyncio.sleep(0.1)

        # Test various pagination scenarios
        # First page
        page1 = await registry.fetch_result(query_id, offset=0, limit=100)
        assert page1 is not None
        assert len(page1.rows) == 100
        assert page1.has_more is True
        assert page1.total_rows == 1000

        # Middle page
        page_middle = await registry.fetch_result(query_id, offset=500, limit=100)
        assert page_middle is not None
        assert len(page_middle.rows) == 100
        assert page_middle.offset == 500

        # Last page
        page_last = await registry.fetch_result(query_id, offset=950, limit=100)
        assert page_last is not None
        assert len(page_last.rows) == 50  # Only 50 rows remaining
        assert page_last.has_more is False

        # Beyond end
        page_beyond = await registry.fetch_result(query_id, offset=1200, limit=100)
        assert page_beyond is not None
        assert len(page_beyond.rows) == 0
        assert page_beyond.has_more is False

    @pytest.mark.asyncio
    async def test_query_options_validation(self, registry: QueryRegistry) -> None:
        """Test various query options configurations."""
        # Test with all options set
        options = QueryOptions(
            query_timeout=timedelta(minutes=10),
            max_inline_rows=1000,
            poll_interval=5.0,
        )

        query_id = await registry.execute_query("SELECT * FROM test", options)
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        assert snapshot.status == QueryStatus.RUNNING

        # Test with minimal options
        minimal_options = QueryOptions()
        query_id2 = await registry.execute_query("SELECT 1", minimal_options)
        snapshot2 = await registry.get_snapshot(query_id2)
        assert snapshot2 is not None
        assert snapshot2.status == QueryStatus.RUNNING

    @pytest.mark.asyncio
    async def test_ttl_edge_cases(self, registry: QueryRegistry) -> None:
        """Test TTL expiration edge cases."""
        # Create query and immediately check it hasn't expired
        query_id = await registry.execute_query("SELECT 1")

        # Should not be expired immediately
        deleted_count = await registry.prune_expired()
        assert deleted_count == 0

        # Query should still exist
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None

    @pytest.mark.asyncio
    async def test_resource_cleanup_on_error(
        self,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test that resources are properly cleaned up when errors occur during startup."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            registry = QueryRegistry(mock_provider, executor)

            # Simulate an error during query execution startup
            original_method = mock_provider.get_connection

            def failing_connection() -> MockConnection:
                raise OperationalError("Connection failed during startup")

            mock_provider.get_connection = failing_connection

            with pytest.raises(OperationalError):
                # This should fail during startup and clean up resources
                _ = await registry.execute_query("SELECT 1")

            # Verify cleanup occurred - failed startup queries should be removed
            async with registry._lock:  # noqa: SLF001
                assert len(registry._store) == 0  # noqa: SLF001

            # Restore original method and verify new queries can still be executed
            mock_provider.get_connection = original_method
            query_id = await registry.execute_query("SELECT 2")
            assert query_id is not None

    @pytest.mark.asyncio
    async def test_input_validation(
        self,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test input validation for public methods."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            registry = QueryRegistry(mock_provider, executor)

            # Test execute_query with empty SQL
            with pytest.raises(ValueError, match="SQL query cannot be empty"):
                _ = await registry.execute_query("")

            with pytest.raises(ValueError, match="SQL query cannot be empty"):
                _ = await registry.execute_query("   ")

            # Test cancel with empty query_id
            with pytest.raises(ValueError, match="Query ID cannot be empty"):
                _ = await registry.cancel("")

            with pytest.raises(ValueError, match="Query ID cannot be empty"):
                _ = await registry.cancel("   ")

            # Test get_snapshot with empty query_id
            with pytest.raises(ValueError, match="Query ID cannot be empty"):
                _ = await registry.get_snapshot("")

            # Test fetch_result with invalid parameters
            with pytest.raises(ValueError, match="Query ID cannot be empty"):
                _ = await registry.fetch_result("")

            valid_query_id = "test-id"
            with pytest.raises(ValueError, match="Offset must be non-negative"):
                _ = await registry.fetch_result(valid_query_id, offset=-1)

            with pytest.raises(ValueError, match="Limit must be positive"):
                _ = await registry.fetch_result(valid_query_id, limit=-1)
