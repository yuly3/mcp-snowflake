import json

import pytest

from mcp_snowflake.handler.list_warehouses import WarehouseInfoDict
from mcp_snowflake.tool.list_warehouses import ListWarehousesTool

from ..mock_effect_handler import MockListWarehouses


class TestListWarehousesTool:
    """Test ListWarehousesTool class."""

    def test_tool_name(self) -> None:
        """Test tool name property."""
        tool = ListWarehousesTool(MockListWarehouses())
        assert tool.name == "list_warehouses"

    def test_tool_definition(self) -> None:
        """Test tool definition."""
        tool = ListWarehousesTool(MockListWarehouses())
        definition = tool.definition

        assert definition.name == "list_warehouses"
        assert definition.description is not None
        assert "warehouses" in definition.description.lower()
        assert definition.inputSchema["type"] == "object"
        assert definition.inputSchema["required"] == []

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful tool execution."""
        mock_warehouses = [
            WarehouseInfoDict(
                name="PROD_WH",
                state="STARTED",
                size="Large",
                owner="ACCOUNTADMIN",
                comment="Production",
            ),
        ]
        tool = ListWarehousesTool(MockListWarehouses(result_data=mock_warehouses))

        result = await tool.perform({})

        assert len(result) == 1
        assert result[0].type == "text"
        response = json.loads(result[0].text)
        assert len(response["warehouses"]) == 1
        assert response["warehouses"][0]["name"] == "PROD_WH"
        assert response["warehouses"][0]["state"] == "STARTED"

    @pytest.mark.asyncio
    async def test_perform_empty_arguments(self) -> None:
        """Test with None arguments."""
        tool = ListWarehousesTool(MockListWarehouses())

        result = await tool.perform(None)

        assert len(result) == 1
        assert result[0].type == "text"
        response = json.loads(result[0].text)
        assert "warehouses" in response

    @pytest.mark.asyncio
    async def test_perform_with_exception(self) -> None:
        """Test tool execution with exception."""
        import mcp.types as types

        tool = ListWarehousesTool(
            MockListWarehouses(should_raise=TimeoutError("timeout"))
        )

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error" in result[0].text
        assert "timeout" in result[0].text.lower()
