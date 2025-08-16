import json
from typing import ClassVar

import mcp.types as types
import pytest
from pydantic import ValidationError

from kernel.table_metadata import TableColumn, TableInfo
from mcp_snowflake.handler import DescribeTableArgs, handle_describe_table


class MockEffectHandler:
    """Mock implementation of EffectDescribeTable protocol."""

    def __init__(
        self,
        table_info: TableInfo | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.table_info = table_info
        self.should_raise = should_raise

    async def describe_table(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table: str,  # noqa: ARG002
    ) -> TableInfo:
        if self.should_raise:
            raise self.should_raise
        if self.table_info is None:
            # Return minimal default
            return TableInfo(
                database="default_db",
                schema="default_schema",
                name="default_table",
                column_count=0,
                columns=[],
            )
        return self.table_info


class TestDescribeTableArgs:
    """Test DescribeTableArgs validation."""

    def test_describe_table_args_validation(self) -> None:
        """Test DescribeTableArgs validation."""
        args = DescribeTableArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )
        assert args.database == "test_db"
        assert args.schema_ == "test_schema"
        assert args.table_ == "test_table"

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"schema": "test_schema", "table": "test_table"}
            )

    def test_missing_schema(self) -> None:
        """Test missing schema argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"database": "test_db", "table": "test_table"}
            )

    def test_missing_table(self) -> None:
        """Test missing table argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"database": "test_db", "schema": "test_schema"}
            )

    def test_missing_all_args(self) -> None:
        """Test missing all arguments."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate({})

    def test_empty_strings(self) -> None:
        """Test empty string arguments."""
        args = DescribeTableArgs(database="", schema="", table="")
        assert args.database == ""
        assert args.schema_ == ""
        assert args.table_ == ""


class TestHandleDescribeTable:
    """Test handle_describe_table function."""

    # Expected keys in describe_table response table_info object
    EXPECTED_RESPONSE_KEYS: ClassVar[set[str]] = {
        "database",
        "schema",
        "name",
        "column_count",
        "columns",
    }

    @pytest.mark.asyncio
    async def test_successful_describe_table(self) -> None:
        """Test successful table description."""
        # Arrange
        args = DescribeTableArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )
        mock_table_data = TableInfo(
            database="test_db",
            schema="test_schema",
            name="test_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="ID",
                    data_type="NUMBER(38,0)",
                    nullable=False,
                    default_value=None,
                    comment="Primary key",
                    ordinal_position=1,
                ),
                TableColumn(
                    name="NAME",
                    data_type="VARCHAR(100)",
                    nullable=True,
                    default_value=None,
                    comment="User name",
                    ordinal_position=2,
                ),
            ],
        )
        effect_handler = MockEffectHandler(table_info=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        response_text = result[0].text

        # Should contain JSON structure
        json_data = json.loads(response_text)
        table_info = json_data["table_info"]

        # Strict key validation - ensure response contains exactly expected keys
        assert set(table_info.keys()) == self.EXPECTED_RESPONSE_KEYS

        assert table_info["database"] == "test_db"
        assert table_info["schema"] == "test_schema"
        assert table_info["name"] == "test_table"
        assert table_info["column_count"] == 2
        assert len(table_info["columns"]) == 2

        # Verify specific columns
        id_column = next(col for col in table_info["columns"] if col["name"] == "ID")
        assert id_column["data_type"] == "NUMBER(38,0)"
        assert id_column["nullable"] is False

        name_column = next(
            col for col in table_info["columns"] if col["name"] == "NAME"
        )
        assert name_column["data_type"] == "VARCHAR(100)"
        assert name_column["nullable"] is True

    @pytest.mark.asyncio
    async def test_empty_table(self) -> None:
        """Test when table has no columns."""
        # Arrange
        args = DescribeTableArgs(
            database="empty_db",
            schema="empty_schema",
            table="empty_table",
        )
        mock_table_data = TableInfo(
            database="empty_db",
            schema="empty_schema",
            name="empty_table",
            column_count=0,
            columns=[],
        )
        effect_handler = MockEffectHandler(table_info=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        response_text = result[0].text

        # Should be valid JSON
        json_data = json.loads(response_text)
        table_info = json_data["table_info"]

        # Strict key validation - ensure response contains exactly expected keys
        assert set(table_info.keys()) == self.EXPECTED_RESPONSE_KEYS

        assert table_info["database"] == "empty_db"
        assert table_info["schema"] == "empty_schema"
        assert table_info["name"] == "empty_table"
        assert table_info["column_count"] == 0
        assert table_info["columns"] == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = DescribeTableArgs(
            database="error_db",
            schema="error_schema",
            table="error_table",
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
            schema="nullable_schema",
            table="nullable_table",
        )
        mock_table_data = TableInfo(
            database="nullable_db",
            schema="nullable_schema",
            name="nullable_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="OPTIONAL1",
                    data_type="VARCHAR(50)",
                    nullable=True,
                    default_value=None,
                    comment=None,
                    ordinal_position=1,
                ),
                TableColumn(
                    name="OPTIONAL2",
                    data_type="INTEGER",
                    nullable=True,
                    default_value="0",
                    comment=None,
                    ordinal_position=2,
                ),
            ],
        )
        effect_handler = MockEffectHandler(table_info=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_text = result[0].text
        json_data = json.loads(response_text)
        table_info = json_data["table_info"]

        # Strict key validation - ensure response contains exactly expected keys
        assert set(table_info.keys()) == self.EXPECTED_RESPONSE_KEYS

        assert table_info["database"] == "nullable_db"
        assert table_info["schema"] == "nullable_schema"
        assert table_info["name"] == "nullable_table"
        assert table_info["column_count"] == 2

        # All columns should be nullable
        for column in table_info["columns"]:
            assert column["nullable"] is True

        # Verify specific columns
        optional1 = next(
            col for col in table_info["columns"] if col["name"] == "OPTIONAL1"
        )
        assert optional1["data_type"] == "VARCHAR(50)"

        optional2 = next(
            col for col in table_info["columns"] if col["name"] == "OPTIONAL2"
        )
        assert optional2["data_type"] == "INTEGER"

    @pytest.mark.asyncio
    async def test_all_required_columns(self) -> None:
        """Test table with all required columns."""
        # Arrange
        args = DescribeTableArgs(
            database="required_db",
            schema="required_schema",
            table="required_table",
        )
        mock_table_data = TableInfo(
            database="required_db",
            schema="required_schema",
            name="required_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="REQUIRED1",
                    data_type="VARCHAR(50)",
                    nullable=False,
                    default_value=None,
                    comment=None,
                    ordinal_position=1,
                ),
                TableColumn(
                    name="REQUIRED2",
                    data_type="INTEGER",
                    nullable=False,
                    default_value=None,
                    comment=None,
                    ordinal_position=2,
                ),
            ],
        )
        effect_handler = MockEffectHandler(table_info=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_text = result[0].text
        json_data = json.loads(response_text)
        table_info = json_data["table_info"]

        # Strict key validation - ensure response contains exactly expected keys
        assert set(table_info.keys()) == self.EXPECTED_RESPONSE_KEYS

        assert table_info["database"] == "required_db"
        assert table_info["schema"] == "required_schema"
        assert table_info["name"] == "required_table"
        assert table_info["column_count"] == 2

        # All columns should be non-nullable (required)
        for column in table_info["columns"]:
            assert column["nullable"] is False

        # Verify specific columns
        required1 = next(
            col for col in table_info["columns"] if col["name"] == "REQUIRED1"
        )
        assert required1["data_type"] == "VARCHAR(50)"

        required2 = next(
            col for col in table_info["columns"] if col["name"] == "REQUIRED2"
        )
        assert required2["data_type"] == "INTEGER"

    @pytest.mark.asyncio
    async def test_pure_json_response(self) -> None:
        """Test that response is pure JSON without any text formatting."""
        # Arrange
        args = DescribeTableArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )
        mock_table_data = TableInfo(
            database="test_db",
            schema="test_schema",
            name="test_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="ID",
                    data_type="NUMBER(38,0)",
                    nullable=False,
                    default_value=None,
                    comment="Primary key",
                    ordinal_position=1,
                ),
                TableColumn(
                    name="NAME",
                    data_type="VARCHAR(100)",
                    nullable=True,
                    default_value=None,
                    comment="Customer name",
                    ordinal_position=2,
                ),
            ],
        )
        effect_handler = MockEffectHandler(table_info=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        response_text = result[0].text

        # Response should be pure JSON
        json_data = json.loads(response_text)  # Should not raise exception

        # Verify JSON structure
        assert "table_info" in json_data
        table_info = json_data["table_info"]

        # Strict key validation - ensure response contains exactly expected keys
        assert set(table_info.keys()) == self.EXPECTED_RESPONSE_KEYS

        assert table_info["database"] == "test_db"
        assert table_info["schema"] == "test_schema"
        assert table_info["name"] == "test_table"
        assert table_info["column_count"] == 2
        assert len(table_info["columns"]) == 2

        # Verify column structure
        id_column = table_info["columns"][0]
        assert id_column["name"] == "ID"
        assert id_column["nullable"] is False

        name_column = table_info["columns"][1]
        assert name_column["name"] == "NAME"
        assert name_column["nullable"] is True

    @pytest.mark.asyncio
    async def test_pure_json_empty_table(self) -> None:
        """Test pure JSON response for empty table."""
        # Arrange
        args = DescribeTableArgs(
            database="empty_db",
            schema="empty_schema",
            table="empty_table",
        )
        mock_table_data = TableInfo(
            database="empty_db",
            schema="empty_schema",
            name="empty_table",
            column_count=0,
            columns=[],
        )
        effect_handler = MockEffectHandler(table_info=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        # Assert
        assert len(result) == 1
        response_text = result[0].text

        # Should be valid JSON
        json_data = json.loads(response_text)
        table_info = json_data["table_info"]

        # Strict key validation - ensure response contains exactly expected keys
        assert set(table_info.keys()) == self.EXPECTED_RESPONSE_KEYS

        assert table_info["column_count"] == 0
        assert table_info["columns"] == []
