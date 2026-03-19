# sample_table_data

Retrieve sample data from a specified table using Snowflake's SAMPLE ROW clause for efficient data sampling.

## Parameters

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `database` | string | yes | — | Database name containing the table |
| `schema` | string | yes | — | Schema name containing the table |
| `table` | string | yes | — | Name of the table to sample |
| `sample_size` | integer | no | 10 | Number of sample rows to retrieve (min: 1) |
| `columns` | array of strings | no | all columns | List of column names to retrieve |

## Example

```json
{
  "name": "sample_table_data",
  "arguments": {
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "table": "ORDERS",
    "sample_size": 5,
    "columns": ["order_id", "customer_id", "total"]
  }
}
```

## Response Format

```
database: MY_DATABASE
schema: PUBLIC
table: ORDERS
sample_size: 5
actual_rows: 3

row1:
order_id: 1001
customer_id: 42
total: 299.99

row2:
order_id: 1002
customer_id: 17
total: 149.50

row3:
order_id: 1003
customer_id: 42
total: 75.00
```

Semi-structured values (objects/arrays) are rendered as inline JSON.
String values are JSON-escaped to keep the compact row structure parseable.
