# execute_query

Execute read-only SQL queries and return structured results. Only SELECT, SHOW, DESCRIBE, EXPLAIN and similar read operations are allowed.

## Parameters

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `sql` | string | yes | — | SQL query to execute (read operations only) |
| `timeout_seconds` | integer | no | 30 | Query timeout in seconds (max: `execute_query.timeout_seconds_max`) |

## Example

```json
{
  "name": "execute_query",
  "arguments": {
    "sql": "SELECT * FROM customers LIMIT 10",
    "timeout_seconds": 60
  }
}
```

## Response Format

```
execution_time_ms: 150
row_count: 2

row1:
id: 1
name: "John"
email: "john@example.com"

row2:
id: 2
name: "Jane"
email: "jane@example.com"
```

Semi-structured values (objects/arrays) are rendered as inline JSON.
String values are always JSON-escaped (e.g., `"john@example.com"`, `"line1\\nline2"`) to keep the row structure parseable.

## Timeout Configuration

See [settings](../settings.md) for `execute_query.timeout_seconds_default` and `execute_query.timeout_seconds_max`.
