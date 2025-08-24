"""SQL generation functionality for table statistics analysis."""

from collections.abc import Iterable

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import DataBase, Schema, Table


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
