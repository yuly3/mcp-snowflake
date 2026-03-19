# profile_semi_structured_columns

Profile semi-structured columns (`VARIANT`, `ARRAY`, `OBJECT`) using sampled recursive flatten analysis.

## Parameters

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `database` | string | yes | — | Database name containing the table |
| `schema` | string | yes | — | Schema name containing the table |
| `table` | string | yes | — | Name of the table to profile |
| `columns` | array of strings | no | auto-select all semi-structured columns | Target columns |
| `sample_rows` | integer | no | 10000 | Sample row count (min: 1, max: 200000) |
| `max_depth` | integer | no | 4 | Maximum recursive path depth (min: 1, max: 20) |
| `top_k_limit` | integer | no | 20 | Top-k limit for frequent values and keys (min: 1, max: 100) |
| `include_path_stats` | boolean | no | true | Include path-level profiling |
| `include_value_samples` | boolean | no | false | Include path-level top_values samples |

## Example

```json
{
  "name": "profile_semi_structured_columns",
  "arguments": {
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "table": "EVENT_LOGS",
    "sample_rows": 10000,
    "max_depth": 4,
    "top_k_limit": 20,
    "include_path_stats": true
  }
}
```

## Response Format

```
database: MY_DATABASE
schema: PUBLIC
table: EVENT_LOGS
total_rows: 100000
sampled_rows: 10000
analyzed_columns: 1

column_profile: payload
column_type: VARIANT
null_count: 100
non_null_count: 9900
null_ratio: 0.01
top_level_type_distribution:
  OBJECT: 8000
  ARRAY: 1000
  STRING: 500
  NUMBER: 300
  BOOLEAN: 100
  NULL: 100

path_profile: payload.user_id
column: payload
path_depth: 1
value_type_distribution:
  STRING: 9500
  NULL: 500
distinct_count_approx: 4200
null_ratio: 0.05

unsupported_columns:
- id (NUMBER)

warnings:
- Path profiling is limited to max_depth=4
```

## Sections

- **Header**: database, schema, table, row counts, analyzed column count
- **column_profile**: null ratio, top-level type distribution, optional array_length_stats, optional top_level_keys_top_k
- **path_profile** (when `include_path_stats` is true): path-wise type distribution, distinct count, null ratio, optional top_values
- **unsupported_columns**: columns skipped due to non-semi-structured types
- **warnings**: sampling/depth-limit and approximation notes

## Timeout Configuration

See [settings](../settings.md) for `profile_semi_structured_columns.base_query_timeout_seconds` and `profile_semi_structured_columns.path_query_timeout_seconds`.
