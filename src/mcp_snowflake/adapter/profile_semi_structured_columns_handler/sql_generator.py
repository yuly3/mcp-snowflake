"""SQL generation for semi-structured column profiling."""

from kernel.sql_utils import fully_qualified, quote_ident
from kernel.table_metadata import DataBase, Schema, Table

NUMBER_TYPE_NAMES = "'NUMBER', 'DECIMAL', 'INT', 'INTEGER', 'FLOAT', 'DOUBLE', 'REAL', 'FIXED'"
STRING_TYPE_NAMES = "'VARCHAR', 'CHAR', 'TEXT', 'STRING'"


def generate_total_rows_sql(database: DataBase, schema: Schema, table: Table) -> str:
    """Generate SQL to count total rows in the target table."""
    table_ref = fully_qualified(database, schema, table)
    return f"SELECT COUNT(*) AS TOTAL_ROWS FROM {table_ref};"  # noqa: S608


def generate_sampled_rows_sql(
    database: DataBase,
    schema: Schema,
    table: Table,
    sample_rows: int,
) -> str:
    """Generate SQL to count sampled rows."""
    table_ref = fully_qualified(database, schema, table)
    return f"SELECT COUNT(*) AS SAMPLED_ROWS FROM {table_ref} SAMPLE ROW ({sample_rows} ROWS);"  # noqa: S608


def generate_column_profile_sql(
    database: DataBase,
    schema: Schema,
    table: Table,
    column_name: str,
    sample_rows: int,
) -> str:
    """Generate SQL for one column-level semi-structured profile."""
    table_ref = fully_qualified(database, schema, table)
    quoted_col = quote_ident(column_name)

    return f"""
WITH sampled AS (
  SELECT {quoted_col} AS value
  FROM {table_ref}
  SAMPLE ROW ({sample_rows} ROWS)
)
SELECT
  SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS NULL_COUNT,
  SUM(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END) AS NON_NULL_COUNT,
  ROUND(DIV0NULL(SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END), COUNT(*)), 6) AS NULL_RATIO,
  OBJECT_CONSTRUCT_KEEP_NULL(
    'OBJECT', SUM(CASE WHEN TYPEOF(value) = 'OBJECT' THEN 1 ELSE 0 END),
    'ARRAY', SUM(CASE WHEN TYPEOF(value) = 'ARRAY' THEN 1 ELSE 0 END),
    'STRING', SUM(CASE WHEN TYPEOF(value) IN ({STRING_TYPE_NAMES}) THEN 1 ELSE 0 END),
    'NUMBER', SUM(CASE WHEN TYPEOF(value) IN ({NUMBER_TYPE_NAMES}) THEN 1 ELSE 0 END),
    'BOOLEAN', SUM(CASE WHEN TYPEOF(value) = 'BOOLEAN' THEN 1 ELSE 0 END),
    'NULL', SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END)
  ) AS TOP_LEVEL_TYPE_DISTRIBUTION,
  MIN(IFF(TYPEOF(value) = 'ARRAY', ARRAY_SIZE(value), NULL)) AS ARRAY_LENGTH_MIN,
  MAX(IFF(TYPEOF(value) = 'ARRAY', ARRAY_SIZE(value), NULL)) AS ARRAY_LENGTH_MAX,
  APPROX_PERCENTILE(IFF(TYPEOF(value) = 'ARRAY', ARRAY_SIZE(value), NULL), 0.25) AS ARRAY_LENGTH_P25,
  APPROX_PERCENTILE(IFF(TYPEOF(value) = 'ARRAY', ARRAY_SIZE(value), NULL), 0.50) AS ARRAY_LENGTH_P50,
  APPROX_PERCENTILE(IFF(TYPEOF(value) = 'ARRAY', ARRAY_SIZE(value), NULL), 0.75) AS ARRAY_LENGTH_P75
FROM sampled;
"""  # noqa: S608


def generate_top_level_keys_sql(
    database: DataBase,
    schema: Schema,
    table: Table,
    column_name: str,
    sample_rows: int,
    top_k_limit: int,
) -> str:
    """Generate SQL for top-level OBJECT keys top-k in one column."""
    table_ref = fully_qualified(database, schema, table)
    quoted_col = quote_ident(column_name)

    return f"""
WITH sampled AS (
  SELECT {quoted_col} AS value
  FROM {table_ref}
  SAMPLE ROW ({sample_rows} ROWS)
),
object_keys AS (
  SELECT f.value::string AS key_name
  FROM sampled,
       LATERAL FLATTEN(input => OBJECT_KEYS(IFF(TYPEOF(value) = 'OBJECT', value, NULL))) f
)
SELECT COALESCE(
  ARRAY_AGG(OBJECT_CONSTRUCT('value', key_name, 'count', key_count)),
  ARRAY_CONSTRUCT()
) AS TOP_LEVEL_KEYS_TOP_K
FROM (
  SELECT key_name, COUNT(*) AS key_count
  FROM object_keys
  GROUP BY key_name
  ORDER BY key_count DESC
  LIMIT {top_k_limit}
);
"""  # noqa: S608


def generate_path_profile_sql(
    database: DataBase,
    schema: Schema,
    table: Table,
    column_name: str,
    sample_rows: int,
    max_depth: int,
    top_k_limit: int,
    include_value_samples: bool,  # noqa: FBT001
) -> str:
    """Generate SQL for recursive path-level profiling in one column."""
    table_ref = fully_qualified(database, schema, table)
    quoted_col = quote_ident(column_name)
    top_values_expr = "COALESCE(tv.top_values, ARRAY_CONSTRUCT())" if include_value_samples else "ARRAY_CONSTRUCT()"
    top_values_cte_template = """
, top_values AS (
  SELECT
    path,
    APPROX_TOP_K(TO_VARCHAR(value), __TOP_K_LIMIT__) AS top_values
  FROM filtered
  WHERE TYPEOF(value) NOT IN ('OBJECT', 'ARRAY', 'NULL')
  GROUP BY path
)
"""
    top_values_cte = (
        top_values_cte_template.replace("__TOP_K_LIMIT__", str(top_k_limit)) if include_value_samples else ""
    )
    top_values_join = "LEFT JOIN top_values tv ON t.path = tv.path" if include_value_samples else ""

    return f"""
WITH sampled AS (
  SELECT {quoted_col} AS root_value
  FROM {table_ref}
  SAMPLE ROW ({sample_rows} ROWS)
),
flattened AS (
  SELECT
    COALESCE(NULLIF(f.path, ''), '$') AS path,
    REGEXP_COUNT(COALESCE(NULLIF(f.path, ''), '$'), '\\\\.|\\\\[') + 1 AS path_depth,
    f.value AS value
  FROM sampled, LATERAL FLATTEN(input => root_value, recursive => true) f
),
filtered AS (
  SELECT path, path_depth, value
  FROM flattened
  WHERE path_depth <= {max_depth}
),
path_agg AS (
  SELECT
    path,
    MAX(path_depth) AS path_depth,
    APPROX_COUNT_DISTINCT(value) AS DISTINCT_COUNT_APPROX,
    ROUND(DIV0NULL(SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END), COUNT(*)), 6) AS NULL_RATIO
  FROM filtered
  GROUP BY path
),
type_agg AS (
  SELECT
    path,
    TYPEOF(value) AS VALUE_TYPE,
    COUNT(*) AS VALUE_COUNT
  FROM filtered
  GROUP BY path, TYPEOF(value)
)
{top_values_cte}
SELECT
  t.path AS PATH,
  p.path_depth AS PATH_DEPTH,
  t.VALUE_TYPE AS VALUE_TYPE,
  t.VALUE_COUNT AS VALUE_COUNT,
  p.DISTINCT_COUNT_APPROX AS DISTINCT_COUNT_APPROX,
  p.NULL_RATIO AS NULL_RATIO,
  {top_values_expr} AS TOP_VALUES
FROM type_agg t
JOIN path_agg p ON t.path = p.path
{top_values_join}
ORDER BY t.path, t.VALUE_COUNT DESC;
"""  # noqa: S608
