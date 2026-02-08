"""Tests for SQL generation in semi-structured profiling adapter."""

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.adapter.profile_semi_structured_columns_handler.sql_generator import (
    generate_column_profile_sql,
    generate_path_profile_sql,
    generate_sampled_rows_sql,
    generate_top_level_keys_sql,
    generate_total_rows_sql,
)


def test_generate_total_and_sampled_rows_sql() -> None:
    """Should generate total/sample count SQL with qualified table names."""
    total_sql = generate_total_rows_sql(
        DataBase("MY_DB"),
        Schema("MY_SCHEMA"),
        Table("MY_TABLE"),
    )
    sampled_sql = generate_sampled_rows_sql(
        DataBase("MY_DB"),
        Schema("MY_SCHEMA"),
        Table("MY_TABLE"),
        1234,
    )

    assert "COUNT(*) AS TOTAL_ROWS" in total_sql
    assert "MY_DB.MY_SCHEMA.MY_TABLE" in total_sql
    assert "SAMPLE ROW (1234 ROWS)" in sampled_sql


def test_generate_column_profile_sql() -> None:
    """Should generate profile SQL with key aggregate functions."""
    sql = generate_column_profile_sql(
        DataBase("TEST_DB"),
        Schema("TEST_SCHEMA"),
        Table("TEST_TABLE"),
        "payload",
        5000,
    )
    assert "OBJECT_CONSTRUCT_KEEP_NULL" in sql
    assert "TOP_LEVEL_TYPE_DISTRIBUTION" in sql
    assert "ARRAY_LENGTH_P50" in sql
    assert "SAMPLE ROW (5000 ROWS)" in sql
    assert 'SELECT "payload" AS value' in sql


def test_generate_top_level_keys_sql() -> None:
    """Should generate top-level key aggregation SQL with top-k limit."""
    sql = generate_top_level_keys_sql(
        DataBase("TEST_DB"),
        Schema("TEST_SCHEMA"),
        Table("TEST_TABLE"),
        "payload",
        5000,
        20,
    )
    assert "OBJECT_KEYS" in sql
    assert "TOP_LEVEL_KEYS_TOP_K" in sql
    assert "LIMIT 20" in sql


def test_generate_path_profile_sql_include_samples() -> None:
    """Should include top_values CTE and join when value samples are enabled."""
    sql = generate_path_profile_sql(
        DataBase("TEST_DB"),
        Schema("TEST_SCHEMA"),
        Table("TEST_TABLE"),
        "payload",
        1000,
        3,
        10,
        True,
    )
    assert "recursive => true" in sql
    assert "path_depth <= 3" in sql
    assert "top_values AS (" in sql
    assert "LEFT JOIN top_values tv ON t.path = tv.path" in sql


def test_generate_path_profile_sql_without_samples() -> None:
    """Should omit top_values CTE/join when value samples are disabled."""
    sql = generate_path_profile_sql(
        DataBase("TEST_DB"),
        Schema("TEST_SCHEMA"),
        Table("TEST_TABLE"),
        "payload",
        1000,
        3,
        10,
        False,
    )
    assert "top_values AS (" not in sql
    assert "LEFT JOIN top_values tv ON t.path = tv.path" not in sql
    assert "ARRAY_CONSTRUCT() AS TOP_VALUES" in sql
