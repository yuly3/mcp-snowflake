"""Tests for ListSchemasEffectHandler."""

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from kernel.table_metadata import DataBase
from mcp_snowflake.adapter.list_schemas_handler import ListSchemasEffectHandler


class TestListSchemasEffectHandler:
    """Test query timeout behavior in ListSchemasEffectHandler."""

    @pytest.mark.asyncio
    async def test_default_query_timeout(self) -> None:
        """Default timeout should be 10 seconds for list_schemas."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(return_value=[{"name": "PUBLIC"}])
        handler = ListSchemasEffectHandler(mock_client)

        _ = await handler.list_schemas(DataBase("MY_DB"))

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=10)

    @pytest.mark.asyncio
    async def test_query_timeout_can_be_configured(self) -> None:
        """Configured timeout should be passed to Snowflake client."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(return_value=[{"name": "PUBLIC"}])
        handler = ListSchemasEffectHandler(mock_client, query_timeout_seconds=30)

        _ = await handler.list_schemas(DataBase("MY_DB"))

        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=30)
