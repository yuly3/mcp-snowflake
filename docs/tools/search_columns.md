# search_columns

Search for columns across tables in a database by column name pattern and/or data type. Results are grouped by table with matched columns returned as a JSON array.

At least one of `column_name_pattern` or `data_type` must be provided.

## Parameters

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `database` | string | yes | — | Database name to search in |
| `column_name_pattern` | string | no | — | Column name ILIKE pattern (e.g. `%unit_id%`) |
| `data_type` | string | no | — | Data type to filter by (e.g. `VARIANT`, `NUMBER`) |
| `schema` | string | no | — | Schema name to filter by |
| `table_name_pattern` | string | no | — | Table name ILIKE pattern (e.g. `%ORDERS%`) |
| `limit` | integer | no | 50 | Maximum number of tables to return (max: 200) |

## Examples

Search by column name pattern:
```json
{
  "name": "search_columns",
  "arguments": {
    "database": "MY_DATABASE",
    "column_name_pattern": "%unit_id%"
  }
}
```

Search by data type within a specific schema:
```json
{
  "name": "search_columns",
  "arguments": {
    "database": "MY_DATABASE",
    "data_type": "VARIANT",
    "schema": "DEV_WAREHOUSE"
  }
}
```

Combined filters:
```json
{
  "name": "search_columns",
  "arguments": {
    "database": "MY_DATABASE",
    "column_name_pattern": "%id%",
    "data_type": "NUMBER",
    "table_name_pattern": "%ORDER%",
    "limit": 20
  }
}
```

## Response Format

Results are grouped by table. Each table's matched columns are returned as a JSON array with `name`, `type`, and optionally `comment` fields.

```
database: MY_DATABASE
table_count: 3

schema: PUBLIC
table: ORDERS
columns: [{"name":"ORDER_ID","type":"NUMBER"},{"name":"CUSTOMER_ID","type":"NUMBER"}]

schema: PUBLIC
table: CUSTOMERS
columns: [{"name":"CUSTOMER_ID","type":"NUMBER","comment":"Primary key"}]

schema: ANALYTICS
table: EVENTS
columns: [{"name":"USER_ID","type":"NUMBER"}]
```

When no tables match:
```
database: MY_DATABASE
table_count: 0
```

The `limit` parameter controls the maximum number of tables returned, not individual columns. The `comment` field is omitted from each column entry when it is null or empty.
