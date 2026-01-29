import json

import pytest

from mcp_snowflake.handler.list_roles import RoleInfoDict
from mcp_snowflake.tool.list_roles import ListRolesTool

from ..mock_effect_handler import MockListRoles


class TestListRolesTool:
    """Test ListRolesTool class."""

    def test_tool_name(self) -> None:
        """Test tool name property."""
        tool = ListRolesTool(MockListRoles())
        assert tool.name == "list_roles"

    def test_tool_definition(self) -> None:
        """Test tool definition."""
        tool = ListRolesTool(MockListRoles())
        definition = tool.definition

        assert definition.name == "list_roles"
        assert definition.description is not None
        assert "roles" in definition.description.lower()
        assert definition.inputSchema["type"] == "object"
        assert definition.inputSchema["required"] == []

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful tool execution."""
        mock_roles = [
            RoleInfoDict(name="ADMIN", owner="ACCOUNTADMIN", comment="Admin role"),
            RoleInfoDict(name="ANALYST", owner="ADMIN", comment=None),
        ]
        tool = ListRolesTool(MockListRoles(result_data=mock_roles))

        result = await tool.perform({})

        assert len(result) == 1
        assert result[0].type == "text"
        response = json.loads(result[0].text)
        assert len(response["roles"]) == 2
        assert response["roles"][0]["name"] == "ADMIN"

    @pytest.mark.asyncio
    async def test_perform_empty_arguments(self) -> None:
        """Test with None arguments."""
        tool = ListRolesTool(MockListRoles())

        result = await tool.perform(None)

        assert len(result) == 1
        assert result[0].type == "text"
        response = json.loads(result[0].text)
        assert "roles" in response

    @pytest.mark.asyncio
    async def test_perform_with_exception(self) -> None:
        """Test tool execution with exception."""
        import mcp.types as types

        tool = ListRolesTool(MockListRoles(should_raise=TimeoutError("timeout")))

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error" in result[0].text
        assert "timeout" in result[0].text.lower()
