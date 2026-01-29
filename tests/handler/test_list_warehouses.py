import pytest

from mcp_snowflake.handler.list_warehouses import (
    ListWarehousesArgs,
    WarehouseInfoDict,
    handle_list_warehouses,
)

from ..mock_effect_handler import MockListWarehouses


class TestListWarehousesArgs:
    """Test ListWarehousesArgs validation."""

    def test_valid_args_empty(self) -> None:
        """Test valid empty arguments."""
        args = ListWarehousesArgs()
        assert args is not None

    def test_valid_args_from_dict(self) -> None:
        """Test creating args from empty dict."""
        args = ListWarehousesArgs.model_validate({})
        assert args is not None


class TestHandleListWarehouses:
    """Test handle_list_warehouses function."""

    @pytest.mark.asyncio
    async def test_successful_list_warehouses(self) -> None:
        """Test successful warehouse listing."""
        # Arrange
        args = ListWarehousesArgs()
        mock_warehouses = [
            WarehouseInfoDict(
                name="PROD_WH",
                state="STARTED",
                size="Large",
                owner="ACCOUNTADMIN",
                comment="Production warehouse",
            ),
            WarehouseInfoDict(
                name="DEV_WH",
                state="SUSPENDED",
                size="Small",
                owner="SYSADMIN",
                comment=None,
            ),
        ]
        effect_handler = MockListWarehouses(result_data=mock_warehouses)

        # Act
        result = await handle_list_warehouses(args, effect_handler)

        # Assert
        assert len(result["warehouses"]) == 2
        assert result["warehouses"][0]["name"] == "PROD_WH"
        assert result["warehouses"][0]["state"] == "STARTED"
        assert result["warehouses"][0]["size"] == "Large"
        assert result["warehouses"][0]["owner"] == "ACCOUNTADMIN"
        assert result["warehouses"][0]["comment"] == "Production warehouse"
        assert result["warehouses"][1]["name"] == "DEV_WH"
        assert result["warehouses"][1]["state"] == "SUSPENDED"

    @pytest.mark.asyncio
    async def test_empty_warehouses_list(self) -> None:
        """Test when no warehouses are returned."""
        # Arrange
        args = ListWarehousesArgs()
        effect_handler = MockListWarehouses(result_data=[])

        # Act
        result = await handle_list_warehouses(args, effect_handler)

        # Assert
        assert result["warehouses"] == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListWarehousesArgs()
        error_message = "Connection failed"
        effect_handler = MockListWarehouses(should_raise=Exception(error_message))

        # Act & Assert
        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_warehouses(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_default_mock_data(self) -> None:
        """Test with default mock data."""
        # Arrange
        args = ListWarehousesArgs()
        effect_handler = MockListWarehouses()

        # Act
        result = await handle_list_warehouses(args, effect_handler)

        # Assert
        assert len(result["warehouses"]) == 2
        assert result["warehouses"][0]["name"] == "COMPUTE_WH"
        assert result["warehouses"][1]["name"] == "DEV_WH"
