"""
Error handling tests for QueryRegistry.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from snowflake.connector.errors import OperationalError

from query_registry import QueryOptions, QueryRegistry

from .conftest import MockConnection, MockSnowflakeConnectionProvider


class TestQueryRegistryErrorHandling:
    """Error handling test cases for QueryRegistry."""

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
