"""AnalyzeTableStatistics EffectHandler implementation."""

from collections.abc import Iterable
from typing import Any

# Import from handler for now - will be moved to kernel in future refactoring
from ..handler.analyze_table_statistics._types import ColumnInfo
from ..snowflake_client import SnowflakeClient
from .describe_table_handler import DescribeTableEffectHandler


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
        database: str,
        schema_name: str,
        table_name: str,
        columns_to_analyze: Iterable[Any],  # ColumnInfo type
        top_k_limit: int,
    ) -> dict[str, Any]:
        """Execute statistics query and return the single result row.

        Parameters
        ----------
        database : str
            Database name
        schema_name : str
            Schema name
        table_name : str
            Table name
        columns_to_analyze : Iterable[Any]
            Column information objects (ColumnInfo)
        top_k_limit : int
            Limit for APPROX_TOP_K function

        Returns
        -------
        dict[str, Any]
            Single row of statistics query results

        Raises
        ------
        ValueError
            If query execution fails or returns no data
        """
        stats_sql = generate_statistics_sql(
            database,
            schema_name,
            table_name,
            columns_to_analyze,
            top_k_limit,
        )

        query_result = await self.client.execute_query(stats_sql)

        if not query_result:
            raise ValueError("No data returned from statistics query")

        return query_result[0]


def generate_statistics_sql(
    database: str,
    schema: str,
    table_name: str,
    columns_info: Iterable[ColumnInfo],
    top_k_limit: int,
) -> str:
    """Generate SQL query for analyzing table statistics.

    Parameters
    ----------
    database : str
        Database name.
    schema : str
        Schema name.
    table_name : str
        Table name.
    columns_info : Iterable[ColumnInfo]
        Column information with type-safe data types.
    top_k_limit : int
        Limit for APPROX_TOP_K function.

    Returns
    -------
    str
        The generated SQL query.
    """
    table_ref = f'"{database}"."{schema}"."{table_name}"'

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
