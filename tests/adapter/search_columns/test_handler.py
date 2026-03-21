"""Tests for SearchColumnsEffectHandler."""

import json
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from kernel.table_metadata import DataBase
from mcp_snowflake.adapter.search_columns_handler import SearchColumnsEffectHandler


class TestSearchColumnsEffectHandler:
    """Test query timeout behavior in SearchColumnsEffectHandler."""

    @pytest.mark.asyncio
    async def test_default_query_timeout(self) -> None:
        """Default timeout should be 30 seconds for search_columns."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(
            return_value=[
                {
                    "TABLE_SCHEMA": "PUBLIC",
                    "TABLE_NAME": "USERS",
                    "COLUMNS": json.dumps([{"name": "ID", "type": "NUMBER", "comment": None}]),
                }
            ]
        )
        handler = SearchColumnsEffectHandler(mock_client)

        _ = await handler.search_columns(DataBase("MY_DB"), "%ID%", None, None, None, 50)

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=30)

    @pytest.mark.asyncio
    async def test_query_timeout_can_be_configured(self) -> None:
        """Configured timeout should be passed to Snowflake client."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(
            return_value=[
                {
                    "TABLE_SCHEMA": "PUBLIC",
                    "TABLE_NAME": "USERS",
                    "COLUMNS": json.dumps([{"name": "ID", "type": "NUMBER", "comment": None}]),
                }
            ]
        )
        handler = SearchColumnsEffectHandler(mock_client, query_timeout_seconds=90)

        _ = await handler.search_columns(DataBase("MY_DB"), "%ID%", None, None, None, 50)

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=90)
