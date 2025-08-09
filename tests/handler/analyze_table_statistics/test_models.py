"""Tests for public API models (AnalyzeTableStatisticsArgs, Protocol)."""

import pytest
from pydantic import ValidationError

from mcp_snowflake.handler.analyze_table_statistics.models import (
    AnalyzeTableStatisticsArgs,
)


class TestAnalyzeTableStatisticsArgs:
    """Test AnalyzeTableStatisticsArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )
        assert args.database == "test_db"
        assert args.schema_name == "test_schema"
        assert args.table_name == "test_table"
        assert args.columns == []
        assert args.top_k_limit == 10

    def test_with_columns(self) -> None:
        """Test with specific columns."""
        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            columns=["col1", "col2"],
        )
        assert args.columns == ["col1", "col2"]

    def test_with_top_k_limit(self) -> None:
        """Test with custom top_k_limit."""
        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            top_k_limit=50,
        )
        assert args.top_k_limit == 50

    def test_top_k_limit_validation(self) -> None:
        """Test top_k_limit validation boundaries."""
        # Valid boundaries
        _ = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            top_k_limit=1,
        )
        _ = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            top_k_limit=100,
        )

        # Invalid boundaries
        with pytest.raises(ValidationError):
            _ = AnalyzeTableStatisticsArgs(
                database="test_db",
                schema_name="test_schema",
                table_name="test_table",
                top_k_limit=0,
            )

        with pytest.raises(ValidationError):
            _ = AnalyzeTableStatisticsArgs(
                database="test_db",
                schema_name="test_schema",
                table_name="test_table",
                top_k_limit=101,
            )
