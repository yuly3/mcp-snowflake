"""
Integration tests for QueryRegistry.
"""

import asyncio

import pytest

from query_registry import QueryRegistry, QueryStatus

from .conftest import MockSnowflakeConnectionProvider


class TestQueryRegistryIntegration:
    """Integration test cases for QueryRegistry."""

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
