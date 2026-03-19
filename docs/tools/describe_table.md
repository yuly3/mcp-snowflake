# describe_table

Retrieve detailed structure information (columns, data types, etc.) for a specified table.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `database` | string | yes | Database name containing the table |
| `schema` | string | yes | Schema name containing the table |
| `table` | string | yes | Name of the table to describe |

## Example

```json
{
  "name": "describe_table",
  "arguments": {
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "table": "CUSTOMERS"
  }
}
```

## Response Format

```
database: MY_DATABASE
schema: PUBLIC
table: CUSTOMERS
column_count: 4

col1:
name: ID
type: NUMBER(38,0)
nullable: false
comment: Primary key

col2:
name: NAME
type: VARCHAR(100)
nullable: true

col3:
name: EMAIL
type: VARCHAR(256)
nullable: true
default: NULL
comment: Contact email

col4:
name: CREATED_AT
type: TIMESTAMP_NTZ(9)
nullable: false
default: CURRENT_TIMESTAMP()
```

Optional fields (`default`, `comment`) are omitted when not set to minimise token usage.
