"""Column analysis test module."""

import mcp.types as types

from mcp_snowflake.handler.analyze_table_statistics._column_analysis import (
    select_and_classify_columns,
)
from mcp_snowflake.kernel.statistics_support_column import StatisticsSupportColumn
from mcp_snowflake.kernel.table_metadata import TableColumn


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

        assert not isinstance(result, types.TextContent)
        supported_columns, unsupported_info = result

        # Verify supported columns (now StatisticsSupportColumn instances)
        assert len(supported_columns) == 2
        assert isinstance(supported_columns[0], StatisticsSupportColumn)
        assert isinstance(supported_columns[1], StatisticsSupportColumn)
        assert supported_columns[0].name == "id"
        assert supported_columns[1].name == "name"

        # Verify unsupported columns
        assert len(unsupported_info) == 2
        unsupported_col_1, reason_1 = unsupported_info[0]
        unsupported_col_2, reason_2 = unsupported_info[1]
        assert unsupported_col_1.name == "metadata"
        assert unsupported_col_1.data_type.raw_type == "VARIANT"
        assert reason_1 == "Unsupported Snowflake data type for statistics: VARIANT"
        assert unsupported_col_2.name == "config"
        assert unsupported_col_2.data_type.raw_type == "OBJECT"
        assert reason_2 == "Unsupported Snowflake data type for statistics: OBJECT"

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

        assert not isinstance(result, types.TextContent)
        supported_columns, unsupported_info = result

        assert len(supported_columns) == 2
        assert all(
            isinstance(col, StatisticsSupportColumn) for col in supported_columns
        )
        assert len(unsupported_info) == 0

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

        assert not isinstance(result, types.TextContent)
        supported_columns, unsupported_info = result

        assert len(supported_columns) == 1
        assert isinstance(supported_columns[0], StatisticsSupportColumn)
        assert supported_columns[0].name == "id"
        assert len(unsupported_info) == 1
        assert unsupported_info[0][0].name == "metadata"

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

        assert isinstance(result, types.TextContent)
        assert "nonexistent" in result.text

    def test_classify_empty_columns_list_returns_error(self) -> None:
        """Test error when no columns are available."""
        all_columns: list[TableColumn] = []
        requested_columns: list[str] = []

        result = select_and_classify_columns(all_columns, requested_columns)

        assert isinstance(result, types.TextContent)
        assert "No columns to analyze" in result.text
