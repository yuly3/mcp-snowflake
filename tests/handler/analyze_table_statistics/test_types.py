from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp_snowflake.handler.analyze_table_statistics._types import (
        BooleanStatsDict,
        TableStatisticsDict,
        UnsupportedColumnDict,
    )


class TestBooleanStatsDict:
    """Tests for BooleanStatsDict TypedDict."""

    def test_boolean_stats_dict_structure(self) -> None:
        """Test BooleanStatsDict structure and required fields."""
        # This test will fail until BooleanStatsDict is implemented
        boolean_stats: BooleanStatsDict = {
            "column_type": "boolean",
            "data_type": "BOOLEAN",
            "count": 950,
            "null_count": 50,
            "true_count": 720,
            "false_count": 230,
            "true_percentage": 75.79,
            "false_percentage": 24.21,
            "true_percentage_with_nulls": 72.0,
            "false_percentage_with_nulls": 23.0,
        }

        # Verify all required fields exist and have correct types
        assert boolean_stats["column_type"] == "boolean"
        assert boolean_stats["data_type"] == "BOOLEAN"
        assert isinstance(boolean_stats["count"], int)
        assert isinstance(boolean_stats["null_count"], int)
        assert isinstance(boolean_stats["true_count"], int)
        assert isinstance(boolean_stats["false_count"], int)
        assert isinstance(boolean_stats["true_percentage"], float)
        assert isinstance(boolean_stats["false_percentage"], float)
        assert isinstance(boolean_stats["true_percentage_with_nulls"], float)
        assert isinstance(boolean_stats["false_percentage_with_nulls"], float)


class TestUnsupportedColumnDict:
    """Tests for UnsupportedColumnDict TypedDict."""

    def test_unsupported_column_dict_structure(self) -> None:
        """Test UnsupportedColumnDict structure and required fields."""
        unsupported_col: UnsupportedColumnDict = {
            "name": "metadata",
            "data_type": "VARIANT",
        }

        # Verify required fields exist and have correct types
        assert unsupported_col["name"] == "metadata"
        assert unsupported_col["data_type"] == "VARIANT"
        assert isinstance(unsupported_col["name"], str)
        assert isinstance(unsupported_col["data_type"], str)


class TestTableStatisticsDictWithUnsupported:
    """Tests for TableStatisticsDict with unsupported_columns field."""

    def test_table_statistics_dict_with_unsupported_columns(self) -> None:
        """Test TableStatisticsDict can include unsupported_columns field."""
        table_stats: TableStatisticsDict = {
            "table_info": {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "total_rows": 1000,
                "analyzed_columns": 2,
            },
            "column_statistics": {
                "id": {
                    "column_type": "numeric",
                    "data_type": "NUMBER(10,0)",
                    "count": 1000,
                    "null_count": 0,
                    "distinct_count_approx": 1000,
                    "min": 1.0,
                    "max": 1000.0,
                    "avg": 500.5,
                    "percentile_25": 250.0,
                    "percentile_50": 500.0,
                    "percentile_75": 750.0,
                }
            },
            "unsupported_columns": [
                {"name": "metadata", "data_type": "VARIANT"},
                {"name": "config", "data_type": "OBJECT"},
            ],
        }

        # Verify structure
        assert "table_info" in table_stats
        assert "column_statistics" in table_stats
        assert "unsupported_columns" in table_stats
        assert len(table_stats["unsupported_columns"]) == 2
        assert table_stats["unsupported_columns"][0]["name"] == "metadata"
        assert table_stats["unsupported_columns"][0]["data_type"] == "VARIANT"

    def test_table_statistics_dict_without_unsupported_columns(self) -> None:
        """Test TableStatisticsDict works without unsupported_columns field."""
        table_stats: TableStatisticsDict = {
            "table_info": {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "total_rows": 1000,
                "analyzed_columns": 1,
            },
            "column_statistics": {
                "id": {
                    "column_type": "numeric",
                    "data_type": "NUMBER(10,0)",
                    "count": 1000,
                    "null_count": 0,
                    "distinct_count_approx": 1000,
                    "min": 1.0,
                    "max": 1000.0,
                    "avg": 500.5,
                    "percentile_25": 250.0,
                    "percentile_50": 500.0,
                    "percentile_75": 750.0,
                }
            },
        }

        # Verify structure works without unsupported_columns
        assert "table_info" in table_stats
        assert "column_statistics" in table_stats
        assert "unsupported_columns" not in table_stats
