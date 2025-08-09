"""SQL generation utilities for table statistics analysis."""

from ._types import ColumnInfo


def generate_statistics_sql(
    database: str,
    schema: str,
    table_name: str,
    columns_info: list[ColumnInfo],
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
    columns_info : list[ColumnInfo]
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

    # Remove trailing comma from the last item
    if sql_parts[-1].endswith(","):
        sql_parts[-1] = sql_parts[-1].removesuffix(",")

    sql_parts.append(f"FROM {table_ref};")

    return "\n".join(sql_parts)
