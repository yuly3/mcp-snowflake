"""Tests for SQL generation functionality."""

from mcp_snowflake.handler.analyze_table_statistics._sql_generator import (
    generate_statistics_sql,
)
from mcp_snowflake.handler.analyze_table_statistics._types import ColumnInfo


class TestGenerateStatisticsSQL:
    """Test generate_statistics_sql function."""

    def test_numeric_column_sql(self) -> None:
        """Test SQL generation for numeric columns."""
        columns_info = [
            ColumnInfo.from_dict({"name": "price", "data_type": "NUMBER(10,2)"}),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            columns_info,
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
            ColumnInfo.from_dict({"name": "status", "data_type": "VARCHAR(10)"}),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            columns_info,
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
            ColumnInfo.from_dict({"name": "created_date", "data_type": "DATE"}),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            columns_info,
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
            ColumnInfo.from_dict({"name": "price", "data_type": "NUMBER(10,2)"}),
            ColumnInfo.from_dict({"name": "status", "data_type": "VARCHAR(10)"}),
            ColumnInfo.from_dict({"name": "created_date", "data_type": "DATE"}),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            columns_info,
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
            ColumnInfo.from_dict(
                {"name": "special-column", "data_type": "NUMBER(10,2)"}
            ),
            ColumnInfo.from_dict(
                {"name": "ORDER", "data_type": "VARCHAR(10)"}
            ),  # Reserved keyword
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            columns_info,
            10,
        )

        # Check that column names are quoted
        assert '"special-column"' in sql
        assert '"ORDER"' in sql

    def test_top_k_limit_parameter(self) -> None:
        """Test that top_k_limit parameter is correctly used."""
        columns_info = [
            ColumnInfo.from_dict({"name": "category", "data_type": "VARCHAR(50)"}),
        ]

        sql = generate_statistics_sql(
            "TEST_DB",
            "TEST_SCHEMA",
            "TEST_TABLE",
            columns_info,
            20,
        )

        assert 'APPROX_TOP_K("category", 20)' in sql

    def test_database_schema_table_escaping(self) -> None:
        """Test that database, schema, and table names are properly escaped."""
        columns_info = [
            ColumnInfo.from_dict({"name": "id", "data_type": "NUMBER(10,0)"}),
        ]

        sql = generate_statistics_sql(
            "my-database",
            "my-schema",
            "my-table",
            columns_info,
            10,
        )

        assert '"my-database"."my-schema"."my-table"' in sql
