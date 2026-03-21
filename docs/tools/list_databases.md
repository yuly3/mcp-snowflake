# list_databases

Retrieve a list of accessible databases.

## Parameters

None.

## Example

```json
{
  "name": "list_databases",
  "arguments": {}
}
```

## Response Format

```
database_count: 3
databases: MY_DB, ANALYTICS, RAW
```

When no databases exist:
```
database_count: 0
databases: (none)
```
