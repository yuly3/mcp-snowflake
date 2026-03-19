# list_schemas

Retrieve a list of schemas from a specified database.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `database` | string | yes | Database name to retrieve schemas from |

## Example

```json
{
  "name": "list_schemas",
  "arguments": {
    "database": "MY_DATABASE"
  }
}
```

## Response Format

```
database: MY_DATABASE
schema_count: 3
schemas: PUBLIC, INFORMATION_SCHEMA, RAW
```

When no schemas exist:
```
database: MY_DATABASE
schema_count: 0
schemas: (none)
```
