"""AnalyzeTableStatistics EffectHandler implementation."""

import logging
from collections.abc import Sequence
from datetime import timedelta

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import DataBase, Schema, Table

from ...handler.analyze_table_statistics.models import TableStatisticsParseResult
from ...snowflake_client import SnowflakeClient
from ..describe_table_handler import DescribeTableEffectHandler
from .result_parser import parse_statistics_result
from .sql_generator import generate_statistics_sql

logger = logging.getLogger(__name__)


class AnalyzeTableStatisticsEffectHandler(DescribeTableEffectHandler):
    """EffectHandler for AnalyzeTableStatistics operations.

    Inherits from DescribeTableEffectHandler to satisfy the
    EffectAnalyzeTableStatistics protocol.
    """

    def __init__(
        self,
        client: SnowflakeClient,
        query_timeout_seconds: int = 60,
    ) -> None:
        """Initialize with SnowflakeClient."""
        super().__init__(client)
        self.query_timeout = timedelta(seconds=query_timeout_seconds)

    async def analyze_table_statistics(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        columns_to_analyze: Sequence[StatisticsSupportColumn],
        top_k_limit: int,
        *,
        include_null_empty_profile: bool,
        include_blank_string_profile: bool,
    ) -> TableStatisticsParseResult:
        """Execute statistics query and return the parsed result.

        Parameters
        ----------
        database : DataBase
            Database name
        schema : Schema
            Schema name
        table : Table
            Table name
        columns_to_analyze : Sequence[StatisticsSupportColumn]
            Column information objects with statistics support
        top_k_limit : int
            Limit for APPROX_TOP_K function
        include_null_empty_profile : bool
            Whether to include quality profile in response
        include_blank_string_profile : bool
            Whether to include TRIM-based blank string profile for string columns

        Returns
        -------
        TableStatisticsParseResult
            Parsed statistics containing total_rows and column statistics

        Raises
        ------
        TimeoutError
            If query execution times out
        ProgrammingError
            SQL syntax errors or other programming errors
        OperationalError
            Database operation related errors
        DataError
            Data processing related errors
        IntegrityError
            Referential integrity constraint violations
        NotSupportedError
            When an unsupported database feature is used
        StatisticsResultParseError
            If the statistics result parsing fails
        """
        stats_sql = generate_statistics_sql(
            database,
            schema,
            table,
            columns_to_analyze,
            top_k_limit,
            include_null_empty_profile=include_null_empty_profile,
            include_blank_string_profile=include_blank_string_profile,
        )

        try:
            query_result = await self.client.execute_query(stats_sql, self.query_timeout)
        except Exception:
            column_properties = [
                {"name": col.base.name, "type": col.statistics_type.type_name} for col in columns_to_analyze
            ]
            logger.exception(
                "failed to analyze table statistics",
                extra={
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "columns": column_properties,
                    "top_k_limit": top_k_limit,
                    "include_null_empty_profile": include_null_empty_profile,
                    "include_blank_string_profile": include_blank_string_profile,
                    "query_timeout_seconds": int(self.query_timeout.total_seconds()),
                    "query": stats_sql,
                },
            )
            raise

        result_row = query_result[0]
        return parse_statistics_result(
            result_row,
            columns_to_analyze,
            include_null_empty_profile=include_null_empty_profile,
            include_blank_string_profile=include_blank_string_profile,
        )
