# analyze_table_statistics

Analyze table statistics using Snowflake's high-performance approximation functions (APPROX_PERCENTILE, APPROX_TOP_K, APPROX_COUNT_DISTINCT) to efficiently retrieve statistical information for numeric, string, date, and boolean columns.

## Parameters

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `database` | string | yes | — | Database name containing the table |
| `schema` | string | yes | — | Schema name containing the table |
| `table` | string | yes | — | Name of the table to analyze |
| `columns` | array of strings | no | all columns | List of column names to analyze |
| `top_k_limit` | integer | no | 10 | Number of top values to retrieve for string columns (max: 100) |
| `include_null_empty_profile` | boolean | no | true | Include per-column quality profile |
| `include_blank_string_profile` | boolean | no | false | Include TRIM-based blank string profile for STRING columns |

## Example

```json
{
  "name": "analyze_table_statistics",
  "arguments": {
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "table": "SALES_DATA",
    "columns": ["amount", "region", "order_date"],
    "top_k_limit": 5,
    "include_null_empty_profile": true,
    "include_blank_string_profile": false
  }
}
```

## Response Format

```
database: MY_DATABASE
schema: PUBLIC
table: SALES_DATA
total_rows: 10000
analyzed_columns: 3

column: amount
column_type: numeric
data_type: NUMBER(10,2)
count: 10000
null_count: 50
distinct_count_approx: 8500
min: 0.01
max: 99999.99
avg: 4532.17
percentile_25: 1200.0
percentile_50: 3500.0
percentile_75: 7800.0
quality_profile:
  null_count: 50
  null_ratio: 0.005

column: region
column_type: string
data_type: VARCHAR(50)
count: 10000
null_count: 0
distinct_count_approx: 5
min_length: 2
max_length: 20
top_values:
- "EAST": 3200
- "WEST": 2800
- "NORTH": 2100

column: order_date
column_type: date
data_type: DATE
count: 10000
null_count: 0
distinct_count_approx: 365
min_date: 2024-01-01
max_date: 2024-12-31
date_range_days: 365

unsupported_columns:
- metadata (VARIANT)

statistics_metadata:
quality_profile_counting_mode: exact
distribution_metrics_mode: approximate
```

## Column Types

| Type | Fields |
|---|---|
| **Numeric** | count, min, max, avg, percentiles (25th, 50th, 75th), distinct_count_approx |
| **String** | count, min_length, max_length, distinct_count_approx, top_values |
| **Date** | count, min_date, max_date, date_range_days, distinct_count_approx |
| **Boolean** | count, true_count, false_count, true/false percentages (NULL-inclusive and exclusive) |

**quality_profile** (optional): null_count, null_ratio; string adds empty_string_count/ratio and optional blank_string_count/ratio.

**statistics_metadata** (when `include_null_empty_profile` is true): counting mode and approximation notes.

## Timeout Configuration

See [settings](../settings.md) for `analyze_table_statistics.query_timeout_seconds`.
