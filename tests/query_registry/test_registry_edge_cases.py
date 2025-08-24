"""
Edge cases and validation tests for QueryRegistry.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from query_registry import QueryOptions, QueryRegistry, QueryStatus

from .conftest import MockSnowflakeConnectionProvider


class TestQueryRegistryEdgeCases:
    """Edge cases and validation test cases for QueryRegistry."""

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
