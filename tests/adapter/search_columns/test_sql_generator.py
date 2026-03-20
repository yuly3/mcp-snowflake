"""Tests for search_columns SQL generation."""

from kernel.table_metadata import DataBase
from mcp_snowflake.adapter.search_columns_handler import _generate_search_columns_sql


class TestGenerateSearchColumnsSQL:
    """Test _generate_search_columns_sql function."""

    def test_column_name_pattern_only(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("MY_DB"),
            column_name_pattern="%unit_id%",
            data_type=None,
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert "COLUMN_NAME ILIKE '%unit_id%'" in sql
        assert "DATA_TYPE =" not in sql
        assert "TABLE_SCHEMA = " not in sql  # Only the != 'INFORMATION_SCHEMA' should exist
        assert "TABLE_NAME ILIKE" not in sql
        assert "MY_DB.INFORMATION_SCHEMA.COLUMNS" in sql

    def test_data_type_only(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("MY_DB"),
            column_name_pattern=None,
            data_type="VARIANT",
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert "DATA_TYPE = 'VARIANT'" in sql
        assert "COLUMN_NAME ILIKE" not in sql

    def test_all_filters(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("MY_DB"),
            column_name_pattern="%id%",
            data_type="NUMBER",
            schema="PUBLIC",
            table_name_pattern="%ORDER%",
            limit=10,
        )
        assert "COLUMN_NAME ILIKE '%id%'" in sql
        assert "DATA_TYPE = 'NUMBER'" in sql
        assert "TABLE_SCHEMA = 'PUBLIC'" in sql
        assert "TABLE_NAME ILIKE '%ORDER%'" in sql
        assert "table_rank <= 10" in sql

    def test_limit_applied_to_table_rank(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("DB"),
            column_name_pattern="%x%",
            data_type=None,
            schema=None,
            table_name_pattern=None,
            limit=200,
        )
        assert "DENSE_RANK() OVER (ORDER BY TABLE_SCHEMA, TABLE_NAME) as table_rank" in sql
        assert "table_rank <= 200" in sql

    def test_single_quote_in_pattern_escaped(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("DB"),
            column_name_pattern="%it's%",
            data_type=None,
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert "COLUMN_NAME ILIKE '%it''s%'" in sql

    def test_single_quote_in_data_type_escaped(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("DB"),
            column_name_pattern=None,
            data_type="VAR'IANT",
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert "DATA_TYPE = 'VAR''IANT'" in sql

    def test_single_quote_in_schema_escaped(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("DB"),
            column_name_pattern="%x%",
            data_type=None,
            schema="SCH'EMA",
            table_name_pattern=None,
            limit=50,
        )
        assert "TABLE_SCHEMA = 'SCH''EMA'" in sql

    def test_nullif_comment_in_sql(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("DB"),
            column_name_pattern="%x%",
            data_type=None,
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert "NULLIF(COMMENT, '') as COMMENT" in sql

    def test_object_construct_in_sql(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("DB"),
            column_name_pattern="%x%",
            data_type=None,
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert "OBJECT_CONSTRUCT('name', COLUMN_NAME, 'type', DATA_TYPE, 'comment', COMMENT)" in sql

    def test_information_schema_excluded(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("DB"),
            column_name_pattern="%x%",
            data_type=None,
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert "TABLE_SCHEMA != 'INFORMATION_SCHEMA'" in sql

    def test_database_with_special_chars_quoted(self) -> None:
        sql = _generate_search_columns_sql(
            DataBase("my-db"),
            column_name_pattern="%x%",
            data_type=None,
            schema=None,
            table_name_pattern=None,
            limit=50,
        )
        assert '"my-db".INFORMATION_SCHEMA.COLUMNS' in sql
