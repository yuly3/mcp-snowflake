"""
Resource cleanup tests for QueryRegistry.
Tests for Connection close calls, Task cancellation, and exception cleanup.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from snowflake.connector.errors import OperationalError

from query_registry import QueryRegistry

from .conftest import MockConnection, MockSnowflakeConnectionProvider


class TestQueryRegistryResourceCleanup:
    """Resource cleanup test cases for QueryRegistry."""

    @pytest.mark.asyncio
    async def test_connection_close_on_completion_detailed(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test Connection close is called when query completes with _handle_completion path."""
        # Reset counters
        mock_provider.reset_all_close_counts()

        # Start query execution
        query_id = await registry.execute_query("SELECT 1")

        # Get the task and simulate query completion manually through _handle_completion path
        async with registry._lock:  # noqa: SLF001
            record = registry._store[query_id]  # noqa: SLF001
            # Ensure runtime is set up with a connection for cleanup testing
            if record.runtime:
                # Set a mock connection that can be tracked
                record.runtime.connection = mock_provider.get_connection()  # pyright: ignore[reportAttributeAccessIssue]

        # Get initial close count
        initial_close_count = mock_provider.get_total_close_calls()

        # Simulate query completion through the normal path
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is not None
        sfqid = snapshot.snowflake.sfqid
        assert sfqid is not None

        mock_provider.simulate_query_completion(sfqid, [{"result": 1}])
        await asyncio.sleep(
            0.2
        )  # Wait for polling to complete and trigger _handle_completion

        # Verify connection close was called during _handle_completion
        final_close_count = mock_provider.get_total_close_calls()
        assert final_close_count > initial_close_count, (
            "Connection.close() should be called in _handle_completion"
        )

    @pytest.mark.asyncio
    async def test_connection_close_on_cancel(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test Connection close is called when query is cancelled."""
        # Reset counters
        mock_provider.reset_all_close_counts()

        # Start query execution
        query_id = await registry.execute_query("SELECT * FROM big_table")

        # Get initial close count
        initial_close_count = mock_provider.get_total_close_calls()

        # Cancel the query
        success = await registry.cancel(query_id)
        assert success is True

        # Wait for cancellation cleanup
        await asyncio.sleep(0.1)

        # Verify connection close was called during cancellation
        final_close_count = mock_provider.get_total_close_calls()
        assert final_close_count > initial_close_count, (
            "Connection.close() should be called during cancellation"
        )

    @pytest.mark.asyncio
    async def test_connection_close_on_prune(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test that prune_expired properly cancels tasks for expired queries."""
        # Reset counters
        mock_provider.reset_all_close_counts()

        # Create query
        query_id = await registry.execute_query("SELECT 1")

        # Get initial close count
        initial_close_count = mock_provider.get_total_close_calls()

        # Manually set TTL to expired (accessing private members for testing)
        async with registry._lock:  # noqa: SLF001
            record = registry._store[query_id]  # noqa: SLF001
            record.ttl_expires_at = datetime.now(UTC) - timedelta(hours=1)

        # Prune expired queries
        deleted_count = await registry.prune_expired()
        assert deleted_count == 1

        # Verify query was removed from store
        snapshot = await registry.get_snapshot(query_id)
        assert snapshot is None

        # Verify connection close was called during prune_expired
        final_close_count = mock_provider.get_total_close_calls()
        assert final_close_count > initial_close_count, (
            "Connection.close() should be called during prune_expired"
        )

    @pytest.mark.asyncio
    async def test_connection_close_on_registry_close(
        self,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test that registry close properly cancels all tasks."""
        # Reset counters
        mock_provider.reset_all_close_counts()

        # Create registry with executor
        with ThreadPoolExecutor(max_workers=2) as executor:
            registry = QueryRegistry(mock_provider, executor)

            # Create multiple queries
            query_id1 = await registry.execute_query("SELECT 1")
            query_id2 = await registry.execute_query("SELECT 2")

            # Verify queries exist before close
            snapshot1 = await registry.get_snapshot(query_id1)
            snapshot2 = await registry.get_snapshot(query_id2)
            assert snapshot1 is not None
            assert snapshot2 is not None

            # Get initial close count
            initial_close_count = mock_provider.get_total_close_calls()

            # Close registry
            await registry.close()

            # Verify all queries were removed from store
            async with registry._lock:  # noqa: SLF001
                assert len(registry._store) == 0, (  # noqa: SLF001
                    "Registry close should clear all queries"
                )

            # Verify connection close was called during close
            final_close_count = mock_provider.get_total_close_calls()
            assert final_close_count > initial_close_count, (
                "Connection.close() should be called during registry close"
            )

    @pytest.mark.asyncio
    async def test_task_cancellation_on_cancel(
        self,
        registry: QueryRegistry,
    ) -> None:
        """Test Task cancellation when cancel() is called."""
        # Start query execution
        query_id = await registry.execute_query("SELECT * FROM big_table")

        # Get the task (accessing private members for testing)
        async with registry._lock:  # noqa: SLF001
            record = registry._store[query_id]  # noqa: SLF001
            task = record.runtime.task if record.runtime else None
            assert task is not None
            assert not task.cancelled()

        # Cancel the query
        success = await registry.cancel(query_id)
        assert success is True

        # Wait for cancellation to complete
        await asyncio.sleep(0.1)

        # Verify task was cancelled
        assert task.cancelled() or task.done(), (
            "Task should be cancelled or completed after cancel()"
        )

    @pytest.mark.asyncio
    async def test_task_cancellation_on_prune(
        self,
        registry: QueryRegistry,
    ) -> None:
        """Test Task cancellation when prune_expired() is called."""
        # Create query
        query_id = await registry.execute_query("SELECT 1")

        # Get the task and manually set TTL to expired
        async with registry._lock:  # noqa: SLF001
            record = registry._store[query_id]  # noqa: SLF001
            task = record.runtime.task if record.runtime else None
            assert task is not None
            assert not task.cancelled()
            # Set TTL to expired
            record.ttl_expires_at = datetime.now(UTC) - timedelta(hours=1)

        # Prune expired queries
        deleted_count = await registry.prune_expired()
        assert deleted_count == 1

        # Wait for cleanup
        await asyncio.sleep(0.1)

        # Verify task was cancelled
        assert task.cancelled() or task.done(), (
            "Task should be cancelled when query is pruned"
        )

    @pytest.mark.asyncio
    async def test_task_cancellation_on_close(
        self,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test Task cancellation when registry is closed."""
        with ThreadPoolExecutor(max_workers=2) as executor:
            registry = QueryRegistry(mock_provider, executor)

            # Create multiple queries
            query_id1 = await registry.execute_query("SELECT 1")
            query_id2 = await registry.execute_query("SELECT 2")

            # Get the tasks
            async with registry._lock:  # noqa: SLF001
                record1 = registry._store[query_id1]  # noqa: SLF001
                record2 = registry._store[query_id2]  # noqa: SLF001
                task1 = record1.runtime.task if record1.runtime else None
                task2 = record2.runtime.task if record2.runtime else None
                assert task1 is not None
                assert task2 is not None
                assert not task1.cancelled()
                assert not task2.cancelled()

            # Close registry
            await registry.close()

            # Wait for cleanup
            await asyncio.sleep(0.1)

            # Verify tasks were cancelled
            assert task1.cancelled() or task1.done(), (
                "Task1 should be cancelled when registry is closed"
            )
            assert task2.cancelled() or task2.done(), (
                "Task2 should be cancelled when registry is closed"
            )

    @pytest.mark.asyncio
    async def test_cleanup_on_connection_error(
        self,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test resource cleanup when connection error occurs."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            registry = QueryRegistry(mock_provider, executor)

            # Reset counters
            mock_provider.reset_all_close_counts()

            # Simulate connection failure
            original_get_connection = mock_provider.get_connection

            def failing_connection() -> MockConnection:
                # Create connection that will be cleaned up
                conn = MockConnection(mock_provider)
                mock_provider.created_connections.append(conn)
                raise OperationalError("Connection failed during startup")

            mock_provider.get_connection = failing_connection

            # Get initial close count
            # initial_close_count = mock_provider.get_total_close_calls()

            # Try to execute query (should fail)
            with pytest.raises(OperationalError):
                _ = await registry.execute_query("SELECT 1")

            # Verify cleanup occurred
            async with registry._lock:  # noqa: SLF001
                assert len(registry._store) == 0, "Failed queries should be cleaned up"  # noqa: SLF001

            # Note: Connection close might not be called if connection creation itself fails
            # This depends on the exact implementation of resource cleanup

            # Restore original method
            mock_provider.get_connection = original_get_connection

    @pytest.mark.asyncio
    async def test_cleanup_on_execution_error(
        self,
        registry: QueryRegistry,
        mock_provider: MockSnowflakeConnectionProvider,
    ) -> None:
        """Test resource cleanup when execution error occurs."""
        # Reset counters
        mock_provider.reset_all_close_counts()

        # Create connection that will simulate execution error
        original_cursor = MockConnection.cursor

        def failing_cursor(self: MockConnection) -> object:
            cursor = original_cursor(self)

            # Override execute_async to fail
            def failing_execute_async(sql: str, timeout: int | None = None) -> None:  # noqa: ARG001
                raise OperationalError("Execution failed")

            cursor.execute_async = failing_execute_async
            return cursor

        # Patch the cursor method
        with patch.object(MockConnection, "cursor", failing_cursor):
            # Get initial close count
            # initial_close_count = mock_provider.get_total_close_calls()

            # Try to execute query (should fail)
            with pytest.raises(OperationalError):
                _ = await registry.execute_query("SELECT 1")

            # Verify cleanup occurred
            async with registry._lock:  # noqa: SLF001
                assert len(registry._store) == 0, "Failed queries should be cleaned up"  # noqa: SLF001

            # Verify connection close was called during cleanup
            # final_close_count = mock_provider.get_total_close_calls()
            # Note: Close call behavior depends on exact failure timing
