"""AnalyzeTableStatistics EffectHandler implementation."""

import logging
from collections.abc import Iterable, Sequence
from typing import Any

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import DataBase, Schema, Table

from ..snowflake_client import SnowflakeClient
from .describe_table_handler import DescribeTableEffectHandler

logger = logging.getLogger(__name__)


class AnalyzeTableStatisticsEffectHandler(DescribeTableEffectHandler):
    """EffectHandler for AnalyzeTableStatistics operations.

    Inherits from DescribeTableEffectHandler to satisfy the
    EffectAnalyzeTableStatistics protocol.
    """

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        super().__init__(client)

    async def analyze_table_statistics(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        columns_to_analyze: Sequence[StatisticsSupportColumn],
        top_k_limit: int,
        role: str | None = None,
        warehouse: str | None = None,
    ) -> dict[str, Any]:
        """Execute statistics query and return the single result row.

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
        role : str | None
            Snowflake role to use for this operation.
        warehouse : str | None
            Snowflake warehouse to use for this operation.

        Returns
        -------
        dict[str, Any]
            Single row of statistics query results

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
        """
        stats_sql = generate_statistics_sql(
            database,
            schema,
            table,
            columns_to_analyze,
            top_k_limit,
        )

        try:
            query_result = await self.client.execute_query(
                stats_sql,
                role=role,
                warehouse=warehouse,
            )
        except Exception:
            column_properties = [
                {"name": col.base.name, "type": col.statistics_type.type_name}
                for col in columns_to_analyze
            ]
            logger.exception(
                "failed to analyze table statistics",
                extra={
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "columns": column_properties,
                    "top_k_limit": top_k_limit,
                    "query": stats_sql,
                },
            )
            raise

        return query_result[0]


def generate_statistics_sql(
    database: DataBase,
    schema: Schema,
    table: Table,
    columns_info: Iterable[StatisticsSupportColumn],
    top_k_limit: int,
) -> str:
    """Generate SQL query for analyzing table statistics.

    Parameters
    ----------
    database : DataBase
        Database name.
    schema : Schema
        Schema name.
    table : Table
        Table name.
    columns_info : Iterable[StatisticsSupportColumn]
        Column information with statistics support guaranteed.
    top_k_limit : int
        Limit for APPROX_TOP_K function.

    Returns
    -------
    str
        The generated SQL query.
    """
    table_ref = f'"{database}"."{schema}"."{table}"'

    sql_parts = ["SELECT", "  COUNT(*) as total_rows,"]

    for col_info in columns_info:
        col_name = col_info.name
        col_type = col_info.statistics_type.type_name

        # Use escaped column names to handle special characters
        escaped_col = f'"{col_name}"'
        prefix = f"{col_type}_{col_name}"

        match col_type:
            case "numeric":
                sql_parts.extend(
                    [
                        f"  COUNT({escaped_col}) as {prefix}_count,",
                        f"  SUM(CASE WHEN {escaped_col} IS NULL THEN 1 ELSE 0 END) as {prefix}_null_count,",
                        f"  MIN({escaped_col}) as {prefix}_min,",
                        f"  MAX({escaped_col}) as {prefix}_max,",
                        f"  AVG({escaped_col}) as {prefix}_avg,",
                        f"  APPROX_PERCENTILE({escaped_col}, 0.25) as {prefix}_q1,",
                        f"  APPROX_PERCENTILE({escaped_col}, 0.5) as {prefix}_median,",
                        f"  APPROX_PERCENTILE({escaped_col}, 0.75) as {prefix}_q3,",
                        f"  APPROX_COUNT_DISTINCT({escaped_col}) as {prefix}_distinct,",
                    ]
                )
            case "string":
                sql_parts.extend(
                    [
                        f"  COUNT({escaped_col}) as {prefix}_count,",
                        f"  SUM(CASE WHEN {escaped_col} IS NULL THEN 1 ELSE 0 END) as {prefix}_null_count,",
                        f"  MIN(LENGTH({escaped_col})) as {prefix}_min_length,",
                        f"  MAX(LENGTH({escaped_col})) as {prefix}_max_length,",
                        f"  APPROX_COUNT_DISTINCT({escaped_col}) as {prefix}_distinct,",
                        f"  APPROX_TOP_K({escaped_col}, {top_k_limit}) as {prefix}_top_values,",
                    ]
                )
            case "date":
                sql_parts.extend(
                    [
                        f"  COUNT({escaped_col}) as {prefix}_count,",
                        f"  SUM(CASE WHEN {escaped_col} IS NULL THEN 1 ELSE 0 END) as {prefix}_null_count,",
                        f"  MIN({escaped_col}) as {prefix}_min,",
                        f"  MAX({escaped_col}) as {prefix}_max,",
                        f"  DATEDIFF('day', MIN({escaped_col}), MAX({escaped_col})) as {prefix}_range_days,",
                        f"  APPROX_COUNT_DISTINCT({escaped_col}) as {prefix}_distinct,",
                    ]
                )
            case "boolean":
                sql_parts.extend(
                    [
                        f"  COUNT({escaped_col}) as {prefix}_count,",
                        f"  SUM(CASE WHEN {escaped_col} IS NULL THEN 1 ELSE 0 END) as {prefix}_null_count,",
                        f"  SUM(CASE WHEN {escaped_col} = TRUE THEN 1 ELSE 0 END) as {prefix}_true_count,",
                        f"  SUM(CASE WHEN {escaped_col} = FALSE THEN 1 ELSE 0 END) as {prefix}_false_count,",
                        f"  ROUND(DIV0NULL(SUM(CASE WHEN {escaped_col} = TRUE THEN 1 ELSE 0 END) * 100.0, COUNT({escaped_col})), 2) as {prefix}_true_percentage,",
                        f"  ROUND(DIV0NULL(SUM(CASE WHEN {escaped_col} = FALSE THEN 1 ELSE 0 END) * 100.0, COUNT({escaped_col})), 2) as {prefix}_false_percentage,",
                        f"  ROUND(DIV0NULL(SUM(CASE WHEN {escaped_col} = TRUE THEN 1 ELSE 0 END) * 100.0, COUNT(*)), 2) as {prefix}_true_percentage_with_nulls,",
                        f"  ROUND(DIV0NULL(SUM(CASE WHEN {escaped_col} = FALSE THEN 1 ELSE 0 END) * 100.0, COUNT(*)), 2) as {prefix}_false_percentage_with_nulls,",
                    ]
                )

    # Remove trailing comma from the last item
    if sql_parts[-1].endswith(","):
        sql_parts[-1] = sql_parts[-1].removesuffix(",")

    sql_parts.append(f"FROM {table_ref};")

    return "\n".join(sql_parts)
