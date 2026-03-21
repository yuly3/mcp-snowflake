"""Tests for SampleTableDataEffectHandler."""

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.adapter.sample_table_data_handler import SampleTableDataEffectHandler


class TestSampleTableDataEffectHandler:
    """Test query timeout behavior in SampleTableDataEffectHandler."""

    @pytest.mark.asyncio
    async def test_default_query_timeout(self) -> None:
        """Default timeout should be 60 seconds for sample_table_data."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(return_value=[{"ID": 1, "NAME": "Alice"}])
        handler = SampleTableDataEffectHandler(mock_client)

        _ = await handler.sample_table_data(DataBase("DB"), Schema("SCH"), Table("TBL"), 10, ["ID", "NAME"])

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=60)

    @pytest.mark.asyncio
    async def test_query_timeout_can_be_configured(self) -> None:
        """Configured timeout should be passed to Snowflake client."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(return_value=[{"ID": 1, "NAME": "Alice"}])
        handler = SampleTableDataEffectHandler(mock_client, query_timeout_seconds=120)

        _ = await handler.sample_table_data(DataBase("DB"), Schema("SCH"), Table("TBL"), 10, ["ID", "NAME"])

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=120)
