"""Tests for DescribeTableEffectHandler."""

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.adapter.describe_table_handler import DescribeTableEffectHandler


class TestDescribeTableEffectHandler:
    """Test query timeout behavior in DescribeTableEffectHandler."""

    @pytest.mark.asyncio
    async def test_default_query_timeout(self) -> None:
        """Default timeout should be 10 seconds for describe_table."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(
            return_value=[
                {"name": "ID", "type": "NUMBER(38,0)", "null?": "N", "default": None, "comment": None},
            ]
        )
        handler = DescribeTableEffectHandler(mock_client)

        _ = await handler.describe_table(DataBase("DB"), Schema("SCH"), Table("TBL"))

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=10)

    @pytest.mark.asyncio
    async def test_query_timeout_can_be_configured(self) -> None:
        """Configured timeout should be passed to Snowflake client."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(
            return_value=[
                {"name": "ID", "type": "NUMBER(38,0)", "null?": "N", "default": None, "comment": None},
            ]
        )
        handler = DescribeTableEffectHandler(mock_client, query_timeout_seconds=60)

        _ = await handler.describe_table(DataBase("DB"), Schema("SCH"), Table("TBL"))

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=60)
