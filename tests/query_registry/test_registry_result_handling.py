"""
Result handling tests for QueryRegistry.
"""

import asyncio

import pytest

from query_registry import QueryRegistry, QueryStatus

from .conftest import MockSnowflakeConnectionProvider


class TestQueryRegistryResultHandling:
    """Result handling test cases for QueryRegistry."""

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
