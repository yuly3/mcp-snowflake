"""Tests for public API models (AnalyzeTableStatisticsArgs, Protocol)."""

import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.analyze_table_statistics.models import (
    AnalyzeTableStatisticsArgs,
)


class TestAnalyzeTableStatisticsArgs:
    """Test AnalyzeTableStatisticsArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
        )
        assert args.database == DataBase("test_db")
        assert args.schema_ == Schema("test_schema")
        assert args.table_ == Table("test_table")
        assert args.columns == []
        assert args.top_k_limit == 10
        assert args.include_null_empty_profile is True
        assert args.include_blank_string_profile is False

    def test_with_columns(self) -> None:
        """Test with specific columns."""
        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            columns=["col1", "col2"],
        )
        assert args.columns == ["col1", "col2"]

    def test_with_top_k_limit(self) -> None:
        """Test with custom top_k_limit."""
        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            top_k_limit=50,
        )
        assert args.top_k_limit == 50

    def test_with_quality_profile_options(self) -> None:
        """Test with quality profile options."""
        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            include_null_empty_profile=False,
            include_blank_string_profile=True,
        )
        assert args.include_null_empty_profile is False
        assert args.include_blank_string_profile is True

    def test_top_k_limit_validation(self) -> None:
        """Test top_k_limit validation boundaries."""
        # Valid boundaries
        _ = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            top_k_limit=1,
        )
        _ = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            top_k_limit=100,
        )

        # Invalid boundaries
        with pytest.raises(ValidationError):
            _ = AnalyzeTableStatisticsArgs(
                database=DataBase("test_db"),
                schema=Schema("test_schema"),
                table=Table("test_table"),
                top_k_limit=0,
            )

        with pytest.raises(ValidationError):
            _ = AnalyzeTableStatisticsArgs(
                database=DataBase("test_db"),
                schema=Schema("test_schema"),
                table=Table("test_table"),
                top_k_limit=101,
            )
