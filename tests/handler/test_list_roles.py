import pytest

from mcp_snowflake.handler.list_roles import (
    ListRolesArgs,
    RoleInfoDict,
    handle_list_roles,
)

from ..mock_effect_handler import MockListRoles


class TestListRolesArgs:
    """Test ListRolesArgs validation."""

    def test_valid_args_empty(self) -> None:
        """Test valid empty arguments."""
        args = ListRolesArgs()
        assert args is not None

    def test_valid_args_from_dict(self) -> None:
        """Test creating args from empty dict."""
        args = ListRolesArgs.model_validate({})
        assert args is not None


class TestHandleListRoles:
    """Test handle_list_roles function."""

    @pytest.mark.asyncio
    async def test_successful_list_roles(self) -> None:
        """Test successful role listing."""
        # Arrange
        args = ListRolesArgs()
        mock_roles = [
            RoleInfoDict(name="ADMIN", owner="ACCOUNTADMIN", comment="Admin role"),
            RoleInfoDict(name="ANALYST", owner="ADMIN", comment=None),
        ]
        effect_handler = MockListRoles(result_data=mock_roles)

        # Act
        result = await handle_list_roles(args, effect_handler)

        # Assert
        assert len(result["roles"]) == 2
        assert result["roles"][0]["name"] == "ADMIN"
        assert result["roles"][0]["owner"] == "ACCOUNTADMIN"
        assert result["roles"][0]["comment"] == "Admin role"
        assert result["roles"][1]["name"] == "ANALYST"
        assert result["roles"][1]["owner"] == "ADMIN"
        assert result["roles"][1]["comment"] is None

    @pytest.mark.asyncio
    async def test_empty_roles_list(self) -> None:
        """Test when no roles are returned."""
        # Arrange
        args = ListRolesArgs()
        effect_handler = MockListRoles(result_data=[])

        # Act
        result = await handle_list_roles(args, effect_handler)

        # Assert
        assert result["roles"] == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListRolesArgs()
        error_message = "Connection failed"
        effect_handler = MockListRoles(should_raise=Exception(error_message))

        # Act & Assert
        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_roles(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_default_mock_data(self) -> None:
        """Test with default mock data."""
        # Arrange
        args = ListRolesArgs()
        effect_handler = MockListRoles()

        # Act
        result = await handle_list_roles(args, effect_handler)

        # Assert
        assert len(result["roles"]) == 2
        assert result["roles"][0]["name"] == "PUBLIC"
        assert result["roles"][1]["name"] == "ACCOUNTADMIN"
