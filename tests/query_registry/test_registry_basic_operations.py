"""
Basic operations tests for QueryRegistry.
"""

from datetime import UTC, datetime, timedelta

import pytest

from query_registry import QueryOptions, QueryRegistry, QueryStatus


class TestQueryRegistryBasicOperations:
    """Basic operations test cases for QueryRegistry."""

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
