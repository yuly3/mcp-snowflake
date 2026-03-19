# list_tables

Retrieve a list of tables and views from a specified database and schema.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `database` | string | yes | Database name to retrieve objects from |
| `schema` | string | yes | Schema name to retrieve objects from |
| `filter` | object | no | Filter for results (see below) |

### Filter

One of:
- Name filter: `{"type": "contains", "value": "<substring>"}` — case-insensitive name substring match
- Object type filter: `{"type": "object_type", "value": "TABLE"}` or `{"type": "object_type", "value": "VIEW"}`

## Examples

```json
{
  "name": "list_tables",
  "arguments": {
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "filter": {
      "type": "contains",
      "value": "order"
    }
  }
}
```

```json
{
  "name": "list_tables",
  "arguments": {
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "filter": {
      "type": "object_type",
      "value": "VIEW"
    }
  }
}
```

## Response Format

```
database: MY_DATABASE
schema: PUBLIC
object_count: 5
tables: CUSTOMERS, ORDERS, PRODUCTS
views: CUSTOMER_VIEW, ORDER_SUMMARY
```

When no objects match:

```
database: MY_DATABASE
schema: PUBLIC
object_count: 0
tables: (none)
views: (none)
```
