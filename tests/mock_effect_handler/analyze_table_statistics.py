"""Mock implementation of EffectAnalyzeTableStatistics protocol."""

from collections.abc import Sequence
from typing import Any

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import DataBase, Schema, Table, TableInfo
from mcp_snowflake.adapter.analyze_table_statistics_handler.result_parser import (
    parse_statistics_result,
)
from mcp_snowflake.handler.analyze_table_statistics.models import (
    TableStatisticsParseResult,
)


class MockAnalyzeTableStatistics:
    """Mock implementation of EffectAnalyzeTableStatistics protocol."""

    def __init__(
        self,
        table_info: TableInfo | None = None,
        statistics_result: dict[str, Any] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.table_info = table_info
        self.statistics_result = statistics_result
        self.should_raise = should_raise

    async def describe_table(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table: str,  # noqa: ARG002
    ) -> TableInfo:
        """Mock describe_table implementation."""
        if self.should_raise:
            raise self.should_raise
        if self.table_info is None:
            # Return minimal default
            return TableInfo(
                database=DataBase("default_db"),
                schema=Schema("default_schema"),
                name="default_table",
                column_count=0,
                columns=[],
            )
        return self.table_info

    async def analyze_table_statistics(
        self,
        database: DataBase,  # noqa: ARG002
        schema: Schema,  # noqa: ARG002
        table: Table,  # noqa: ARG002
        columns_to_analyze: Sequence[StatisticsSupportColumn],
        top_k_limit: int,  # noqa: ARG002
        *,
        include_null_empty_profile: bool,
        include_blank_string_profile: bool,
    ) -> TableStatisticsParseResult:
        """Mock analyze_table_statistics implementation."""
        if self.should_raise:
            raise self.should_raise

        statistics_result = self.statistics_result
        if statistics_result is None:
            # Return default statistics result
            statistics_result = {
                "TOTAL_ROWS": 1000,
                # Add default column statistics if needed
            }

        # Parse the dict result using the moved parser
        return parse_statistics_result(
            statistics_result,
            columns_to_analyze,
            include_null_empty_profile=include_null_empty_profile,
            include_blank_string_profile=include_blank_string_profile,
        )
