from typing import Any

import mcp.types as types
import pytest
from pydantic import ValidationError

from mcp_snowflake.handler import DescribeTableArgs, handle_describe_table


class MockEffectHandler:
    """Mock implementation of _EffectDescribeTable protocol."""

    def __init__(
        self,
        table_data: dict[str, Any] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.table_data = table_data or {}
        self.should_raise = should_raise

    async def describe_table(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table_name: str,  # noqa: ARG002
    ) -> dict[str, Any]:
        if self.should_raise:
            raise self.should_raise
        return self.table_data


class TestDescribeTableArgs:
    """Test DescribeTableArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = DescribeTableArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )
        assert args.database == "test_db"
        assert args.schema_name == "test_schema"
        assert args.table_name == "test_table"

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"schema_name": "test_schema", "table_name": "test_table"}
            )

    def test_missing_schema_name(self) -> None:
        """Test missing schema_name argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"database": "test_db", "table_name": "test_table"}
            )

    def test_missing_table_name(self) -> None:
        """Test missing table_name argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"database": "test_db", "schema_name": "test_schema"}
            )

    def test_missing_all_args(self) -> None:
        """Test missing all arguments."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate({})

    def test_empty_strings(self) -> None:
        """Test empty string arguments."""
        args = DescribeTableArgs(database="", schema_name="", table_name="")
        assert args.database == ""
        assert args.schema_name == ""
        assert args.table_name == ""


class TestHandleDescribeTable:
    """Test handle_describe_table function."""

    @pytest.mark.asyncio
    async def test_successful_describe_table(self) -> None:
        """Test successful table description."""
        # Arrange
        args = DescribeTableArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )
        mock_table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 2,
            "columns": [
                {
                    "name": "ID",
                    "data_type": "NUMBER(38,0)",
                    "nullable": False,
                    "default_value": None,
                    "comment": "Primary key",
                    "ordinal_position": 1,
                },
                {
                    "name": "NAME",
                    "data_type": "VARCHAR(100)",
                    "nullable": True,
                    "default_value": None,
                    "comment": "User name",
                    "ordinal_position": 2,
                },
            ],
        }
        effect_handler = MockEffectHandler(table_data=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Table Schema: test_db.test_schema.test_table" in result[0].text
        assert "This table has 2 columns" in result[0].text
        assert '"name": "ID"' in result[0].text
        assert '"name": "NAME"' in result[0].text
        assert "Primary key: ID" in result[0].text
        assert "Required fields: ID" in result[0].text
        assert "Optional fields: NAME" in result[0].text

    @pytest.mark.asyncio
    async def test_empty_table(self) -> None:
        """Test when table has no columns."""
        # Arrange
        args = DescribeTableArgs(
            database="empty_db",
            schema_name="empty_schema",
            table_name="empty_table",
        )
        mock_table_data = {
            "database": "empty_db",
            "schema_name": "empty_schema",
            "name": "empty_table",
            "column_count": 0,
            "columns": [],
        }
        effect_handler = MockEffectHandler(table_data=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Table Schema: empty_db.empty_schema.empty_table" in result[0].text
        assert "This table has 0 columns" in result[0].text
        assert "Required fields: None" in result[0].text
        assert "Optional fields: None" in result[0].text

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = DescribeTableArgs(
            database="error_db",
            schema_name="error_schema",
            table_name="error_table",
        )
        error_message = "Table not found"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Failed to describe table:" in result[0].text
        assert error_message in result[0].text

    @pytest.mark.asyncio
    async def test_all_nullable_columns(self) -> None:
        """Test table with all nullable columns."""
        # Arrange
        args = DescribeTableArgs(
            database="nullable_db",
            schema_name="nullable_schema",
            table_name="nullable_table",
        )
        mock_table_data = {
            "database": "nullable_db",
            "schema_name": "nullable_schema",
            "name": "nullable_table",
            "column_count": 2,
            "columns": [
                {
                    "name": "OPTIONAL1",
                    "data_type": "VARCHAR(50)",
                    "nullable": True,
                    "default_value": None,
                    "comment": None,
                    "ordinal_position": 1,
                },
                {
                    "name": "OPTIONAL2",
                    "data_type": "INTEGER",
                    "nullable": True,
                    "default_value": "0",
                    "comment": None,
                    "ordinal_position": 2,
                },
            ],
        }
        effect_handler = MockEffectHandler(table_data=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Required fields: None" in result[0].text
        assert "Optional fields: OPTIONAL1, OPTIONAL2" in result[0].text
        assert "Primary key: Not identified" in result[0].text

    @pytest.mark.asyncio
    async def test_all_required_columns(self) -> None:
        """Test table with all required columns."""
        # Arrange
        args = DescribeTableArgs(
            database="required_db",
            schema_name="required_schema",
            table_name="required_table",
        )
        mock_table_data = {
            "database": "required_db",
            "schema_name": "required_schema",
            "name": "required_table",
            "column_count": 2,
            "columns": [
                {
                    "name": "REQUIRED1",
                    "data_type": "VARCHAR(50)",
                    "nullable": False,
                    "default_value": None,
                    "comment": None,
                    "ordinal_position": 1,
                },
                {
                    "name": "REQUIRED2",
                    "data_type": "INTEGER",
                    "nullable": False,
                    "default_value": None,
                    "comment": None,
                    "ordinal_position": 2,
                },
            ],
        }
        effect_handler = MockEffectHandler(table_data=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Required fields: REQUIRED1, REQUIRED2" in result[0].text
        assert "Optional fields: None" in result[0].text
        assert "Primary key: REQUIRED1" in result[0].text  # First required column
