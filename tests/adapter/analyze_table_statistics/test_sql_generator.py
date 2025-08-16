"""Tests for SQL generation functionality in adapter layer."""

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import TableColumn
from mcp_snowflake.adapter.analyze_table_statistics_handler import (
    generate_statistics_sql,
)


def _convert_to_statistics_support_columns(
    columns: list[TableColumn],
) -> list[StatisticsSupportColumn]:
    """Convert TableColumns to StatisticsSupportColumns for testing."""
    result = []
    for col in columns:
        stats_col = StatisticsSupportColumn.from_table_column(col)
        if stats_col is not None:
            result.append(stats_col)
    return result


class TestGenerateStatisticsSQL:
    """Test generate_statistics_sql function."""

    def test_numeric_column_sql(self) -> None:
        """Test SQL generation for numeric columns."""
        columns_info = [
            TableColumn(
                name="price",
                data_type="DECIMAL(10,2)",
                nullable=True,
                ordinal_position=2,
            ),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            _convert_to_statistics_support_columns(columns_info),
            10,
        )

        # Check basic structure
        assert "COUNT(*) as total_rows" in sql
        assert '"TEST_DB"."TEST_SCHEMA"."TEST_TABLE"' in sql

        # Check numeric-specific aggregations
        assert 'COUNT("price") as numeric_price_count' in sql
        assert 'MIN("price") as numeric_price_min' in sql
        assert 'MAX("price") as numeric_price_max' in sql
        assert 'AVG("price") as numeric_price_avg' in sql
        assert 'APPROX_PERCENTILE("price", 0.25) as numeric_price_q1' in sql
        assert 'APPROX_PERCENTILE("price", 0.5) as numeric_price_median' in sql
        assert 'APPROX_PERCENTILE("price", 0.75) as numeric_price_q3' in sql
        assert 'APPROX_COUNT_DISTINCT("price") as numeric_price_distinct' in sql

    def test_string_column_sql(self) -> None:
        """Test SQL generation for string columns."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            _convert_to_statistics_support_columns(columns_info),
            5,
        )

        # Check string-specific aggregations
        assert 'COUNT("status") as string_status_count' in sql
        assert 'MIN(LENGTH("status")) as string_status_min_length' in sql
        assert 'MAX(LENGTH("status")) as string_status_max_length' in sql
        assert 'APPROX_COUNT_DISTINCT("status") as string_status_distinct' in sql
        assert 'APPROX_TOP_K("status", 5) as string_status_top_values' in sql

    def test_date_column_sql(self) -> None:
        """Test SQL generation for date columns."""
        columns_info = [
            TableColumn(
                name="created_date",
                data_type="DATE",
                nullable=False,
                ordinal_position=3,
            ),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            _convert_to_statistics_support_columns(columns_info),
            10,
        )

        # Check date-specific aggregations
        assert 'COUNT("created_date") as date_created_date_count' in sql
        assert 'MIN("created_date") as date_created_date_min' in sql
        assert 'MAX("created_date") as date_created_date_max' in sql
        assert (
            'DATEDIFF(\'day\', MIN("created_date"), MAX("created_date")) as date_created_date_range_days'
            in sql
        )
        assert (
            'APPROX_COUNT_DISTINCT("created_date") as date_created_date_distinct' in sql
        )

    def test_mixed_column_types(self) -> None:
        """Test SQL generation with mixed column types."""
        columns_info = [
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=1,
            ),
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=2,
            ),
            TableColumn(
                name="created_date",
                data_type="DATE",
                nullable=False,
                ordinal_position=3,
            ),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            _convert_to_statistics_support_columns(columns_info),
            5,
        )

        # Check basic structure
        assert "COUNT(*) as total_rows" in sql
        assert '"TEST_DB"."TEST_SCHEMA"."TEST_TABLE"' in sql

        # Check all column types are present
        assert 'APPROX_PERCENTILE("price", 0.25)' in sql
        assert 'APPROX_TOP_K("status", 5)' in sql
        assert 'DATEDIFF(\'day\', MIN("created_date"), MAX("created_date"))' in sql

        # Ensure no trailing comma
        assert not sql.rstrip().endswith(",")

    def test_column_name_escaping(self) -> None:
        """Test that column names are properly escaped."""
        columns_info = [
            TableColumn(
                name="special-column",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=1,
            ),
            TableColumn(
                name="ORDER",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=2,
            ),  # Reserved keyword
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            _convert_to_statistics_support_columns(columns_info),
            10,
        )

        # Check that column names are quoted
        assert '"special-column"' in sql
        assert '"ORDER"' in sql

    def test_top_k_limit_parameter(self) -> None:
        """Test that top_k_limit parameter is correctly used."""
        columns_info = [
            TableColumn(
                name="category",
                data_type="VARCHAR(50)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            _convert_to_statistics_support_columns(columns_info),
            20,
        )

        assert 'APPROX_TOP_K("category", 20)' in sql

    def test_database_schema_table_escaping(self) -> None:
        """Test that database, schema, and table names are properly escaped."""
        columns_info = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        sql = generate_statistics_sql(
            "my-database",
            "my-schema",
            "my-table",
            _convert_to_statistics_support_columns(columns_info),
            10,
        )

        assert '"my-database"."my-schema"."my-table"' in sql

    def test_boolean_column_sql(self) -> None:
        """Test SQL generation for boolean columns."""
        columns_info = [
            TableColumn(
                name="is_active",
                data_type="BOOLEAN",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            _convert_to_statistics_support_columns(columns_info),
            10,
        )

        # Check basic structure
        assert "COUNT(*) as total_rows" in sql
        assert '"TEST_DB"."TEST_SCHEMA"."TEST_TABLE"' in sql

        # Check boolean-specific aggregations
        assert 'COUNT("is_active") as boolean_is_active_count' in sql
        assert (
            'SUM(CASE WHEN "is_active" IS NULL THEN 1 ELSE 0 END) as boolean_is_active_null_count'
            in sql
        )
        assert (
            'SUM(CASE WHEN "is_active" = TRUE THEN 1 ELSE 0 END) as boolean_is_active_true_count'
            in sql
        )
        assert (
            'SUM(CASE WHEN "is_active" = FALSE THEN 1 ELSE 0 END) as boolean_is_active_false_count'
            in sql
        )

        # Check percentage calculations with DIV0NULL
        assert (
            'ROUND(DIV0NULL(SUM(CASE WHEN "is_active" = TRUE THEN 1 ELSE 0 END) * 100.0, COUNT("is_active")), 2) as boolean_is_active_true_percentage'
            in sql
        )
        assert (
            'ROUND(DIV0NULL(SUM(CASE WHEN "is_active" = FALSE THEN 1 ELSE 0 END) * 100.0, COUNT("is_active")), 2) as boolean_is_active_false_percentage'
            in sql
        )

        # Check percentage calculations with nulls included
        assert (
            'ROUND(DIV0NULL(SUM(CASE WHEN "is_active" = TRUE THEN 1 ELSE 0 END) * 100.0, COUNT(*)), 2) as boolean_is_active_true_percentage_with_nulls'
            in sql
        )
        assert (
            'ROUND(DIV0NULL(SUM(CASE WHEN "is_active" = FALSE THEN 1 ELSE 0 END) * 100.0, COUNT(*)), 2) as boolean_is_active_false_percentage_with_nulls'
            in sql
        )
