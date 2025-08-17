"""Column analysis test module."""

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import TableColumn
from mcp_snowflake.handler.analyze_table_statistics import (
    ColumnDoesNotExist,
    NoSupportedColumns,
)
from mcp_snowflake.handler.analyze_table_statistics._column_analysis import (
    select_and_classify_columns,
)
from mcp_snowflake.handler.analyze_table_statistics._types import ClassifiedColumns


class TestSelectAndClassifyColumns:
    """Test select_and_classify_columns function."""

    def test_classify_supported_and_unsupported_columns(self) -> None:
        """Test classification of supported and unsupported columns."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="name",
                data_type="VARCHAR(50)",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
            TableColumn(
                name="metadata",
                data_type="VARIANT",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=3,
            ),
            TableColumn(
                name="config",
                data_type="OBJECT",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=4,
            ),
        ]
        requested_columns: list[str] = []

        result = select_and_classify_columns(all_columns, requested_columns)

        assert isinstance(result, ClassifiedColumns)
        supported_columns = result.supported_columns
        unsupported_columns = result.unsupported_columns

        # Verify supported columns (now StatisticsSupportColumn instances)
        assert len(supported_columns) == 2
        assert isinstance(supported_columns[0], StatisticsSupportColumn)
        assert isinstance(supported_columns[1], StatisticsSupportColumn)
        assert supported_columns[0].name == "id"
        assert supported_columns[1].name == "name"

        # Verify unsupported columns
        assert len(unsupported_columns) == 2
        unsupported_col_1 = unsupported_columns[0]
        unsupported_col_2 = unsupported_columns[1]
        assert unsupported_col_1.name == "metadata"
        assert unsupported_col_1.data_type.raw_type == "VARIANT"
        assert unsupported_col_2.name == "config"
        assert unsupported_col_2.data_type.raw_type == "OBJECT"

    def test_classify_all_supported_columns(self) -> None:
        """Test classification when all columns are supported."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="name",
                data_type="VARCHAR(50)",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
        ]
        requested_columns: list[str] = []

        result = select_and_classify_columns(all_columns, requested_columns)

        assert isinstance(result, ClassifiedColumns)
        supported_columns = result.supported_columns
        unsupported_columns = result.unsupported_columns

        assert len(supported_columns) == 2
        assert all(
            isinstance(col, StatisticsSupportColumn) for col in supported_columns
        )
        assert len(unsupported_columns) == 0

    def test_classify_requested_columns_with_mixed_support(self) -> None:
        """Test classification with specific requested columns."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="metadata",
                data_type="VARIANT",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
        ]
        requested_columns = ["id", "metadata"]

        result = select_and_classify_columns(all_columns, requested_columns)

        assert isinstance(result, ClassifiedColumns)
        supported_columns = result.supported_columns
        unsupported_columns = result.unsupported_columns

        assert len(supported_columns) == 1
        assert isinstance(supported_columns[0], StatisticsSupportColumn)
        assert supported_columns[0].name == "id"
        assert len(unsupported_columns) == 1
        assert unsupported_columns[0].name == "metadata"

    def test_classify_missing_columns_returns_error(self) -> None:
        """Test error when requested columns don't exist."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
        ]
        requested_columns = ["id", "nonexistent"]

        result = select_and_classify_columns(all_columns, requested_columns)

        assert isinstance(result, ColumnDoesNotExist)
        assert "nonexistent" in result.not_existed_columns

    def test_no_supported_columns(self) -> None:
        """Test that only unsupported columns return NoSupportedColumns."""
        # Create columns with unsupported data types
        unsupported_columns = [
            TableColumn(
                name="JSON_DATA",
                data_type="VARIANT",
                nullable=True,
                default_value=None,
                comment="JSON data",
                ordinal_position=1,
            ),
            TableColumn(
                name="BINARY_DATA",
                data_type="BINARY",
                nullable=True,
                default_value=None,
                comment="Binary data",
                ordinal_position=2,
            ),
            TableColumn(
                name="OBJECT_DATA",
                data_type="OBJECT",
                nullable=True,
                default_value=None,
                comment="Object data",
                ordinal_position=3,
            ),
        ]

        # Test with empty requested_columns (analyze all)
        result = select_and_classify_columns(unsupported_columns, [])

        # Should return NoSupportedColumns with all unsupported columns
        assert isinstance(result, NoSupportedColumns)
        assert len(result.unsupported_columns) == 3
        assert result.unsupported_columns == unsupported_columns

    def test_missing_columns_priority_over_unsupported(self) -> None:
        """Test that missing columns take priority over unsupported columns."""
        available_columns = [
            TableColumn(
                name="BINARY_DATA",
                data_type="BINARY",  # Unsupported
                nullable=True,
                default_value=None,
                comment="Binary data",
                ordinal_position=1,
            ),
        ]

        # Request both existing (unsupported) and missing columns
        requested_columns = ["BINARY_DATA", "NONEXISTENT"]
        result = select_and_classify_columns(available_columns, requested_columns)

        # Should return ColumnDoesNotExist (missing takes priority)
        assert isinstance(result, ColumnDoesNotExist)
        assert "NONEXISTENT" in result.not_existed_columns
        assert len(result.existed_columns) == 1
        assert result.existed_columns[0].name == "BINARY_DATA"
