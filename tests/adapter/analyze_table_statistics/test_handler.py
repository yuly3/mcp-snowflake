"""Tests for AnalyzeTableStatisticsEffectHandler."""

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import DataBase, Schema, Table, TableColumn
from mcp_snowflake.adapter.analyze_table_statistics_handler.handler import (
    AnalyzeTableStatisticsEffectHandler,
)


def _convert_to_statistics_support_columns(
    columns: list[TableColumn],
) -> list[StatisticsSupportColumn]:
    result: list[StatisticsSupportColumn] = []
    for col in columns:
        stats_col = StatisticsSupportColumn.from_table_column(col)
        if stats_col is not None:
            result.append(stats_col)
    return result


class TestAnalyzeTableStatisticsEffectHandler:
    """Test query timeout behavior in AnalyzeTableStatisticsEffectHandler."""

    @pytest.mark.asyncio
    async def test_default_query_timeout_is_relaxed_to_60_seconds(self) -> None:
        """Default timeout should be 60 seconds for analyze_table_statistics."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(
            return_value=[
                {
                    "TOTAL_ROWS": 100,
                    "NUMERIC_PRICE_COUNT": 100,
                    "NUMERIC_PRICE_NULL_COUNT": 0,
                    "NUMERIC_PRICE_MIN": 1.0,
                    "NUMERIC_PRICE_MAX": 100.0,
                    "NUMERIC_PRICE_AVG": 50.0,
                    "NUMERIC_PRICE_Q1": 25.0,
                    "NUMERIC_PRICE_MEDIAN": 50.0,
                    "NUMERIC_PRICE_Q3": 75.0,
                    "NUMERIC_PRICE_DISTINCT": 100,
                }
            ]
        )
        handler = AnalyzeTableStatisticsEffectHandler(mock_client)

        columns = _convert_to_statistics_support_columns([
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=True,
                ordinal_position=1,
            ),
        ])

        _ = await handler.analyze_table_statistics(
            DataBase("TEST_DB"),
            Schema("PUBLIC"),
            Table("SALES"),
            columns,
            10,
            include_null_empty_profile=False,
            include_blank_string_profile=False,
        )

        assert mock_client.execute_query.await_count == 1
        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=60)

    @pytest.mark.asyncio
    async def test_query_timeout_can_be_configured(self) -> None:
        """Configured timeout should be passed to Snowflake client."""
        mock_client = Mock()
        mock_client.execute_query = AsyncMock(
            return_value=[
                {
                    "TOTAL_ROWS": 100,
                    "NUMERIC_PRICE_COUNT": 100,
                    "NUMERIC_PRICE_NULL_COUNT": 0,
                    "NUMERIC_PRICE_MIN": 1.0,
                    "NUMERIC_PRICE_MAX": 100.0,
                    "NUMERIC_PRICE_AVG": 50.0,
                    "NUMERIC_PRICE_Q1": 25.0,
                    "NUMERIC_PRICE_MEDIAN": 50.0,
                    "NUMERIC_PRICE_Q3": 75.0,
                    "NUMERIC_PRICE_DISTINCT": 100,
                }
            ]
        )
        handler = AnalyzeTableStatisticsEffectHandler(mock_client, query_timeout_seconds=180)

        columns = _convert_to_statistics_support_columns([
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=True,
                ordinal_position=1,
            ),
        ])

        _ = await handler.analyze_table_statistics(
            DataBase("TEST_DB"),
            Schema("PUBLIC"),
            Table("SALES"),
            columns,
            10,
            include_null_empty_profile=False,
            include_blank_string_profile=False,
        )

        assert mock_client.execute_query.await_count == 1
        _, timeout = mock_client.execute_query.await_args.args
        assert timeout == timedelta(seconds=180)
