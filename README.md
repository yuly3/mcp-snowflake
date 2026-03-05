# MCP Snowflake Server

A Model Context Protocol (MCP) server that connects to Snowflake databases and executes SQL queries.

## Features

- **List Schemas**: Retrieve a list of schemas from a specified database
- **List Tables**: Retrieve a list of tables from a specified database and schema
- **List Views**: Retrieve a list of views from a specified database and schema
- **Describe Table**: Retrieve detailed structure information for a specified table
- **Execute Query**: Execute read-only SQL queries and return results
- **Sample Table Data**: Retrieve sample data from a specified table using Snowflake's SAMPLE ROW clause
- **Analyze Table Statistics**: Generate comprehensive statistical analysis for table columns using Snowflake's high-performance approximation functions (supports numeric, string, date, and boolean columns)
- **Profile Semi-Structured Columns**: Profile VARIANT/ARRAY/OBJECT columns with sampled flatten-based analysis

## Installation

### Prerequisites

- Python 3.13 or higher
- uv (Python package manager)
- Access to a Snowflake account

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd mcp-snowflake
```

2. Install using uv:
```bash
uv tool install -e .
```

## Configuration

### Using Configuration File (Recommended)

1. Copy the configuration file sample:
```bash
cp .mcp_snowflake.toml.example .mcp_snowflake.toml
```

2. Edit `.mcp_snowflake.toml` to configure your Snowflake connection:
```toml
[snowflake]
account = "your-account.region"
user = "your-username"
password = "your-password"  # Required only when authenticator = "SNOWFLAKE"
warehouse = "your-warehouse"  # Optional
role = "your-role"  # Optional
secondary_roles = ["analyst_role", "bi_reader_role"]  # Optional: role names for USE SECONDARY ROLES
# secondary_roles = ["NONE"]  # Optional: explicitly disable secondary roles
authenticator = "SNOWFLAKE"  # "SNOWFLAKE" or "externalbrowser"
client_store_temporary_credential = true  # ID token cache for externalbrowser

[tools]
# Enable/disable specific tools (all enabled by default)
analyze_table_statistics = true  # Optional
describe_table = true  # Optional
execute_query = true  # Optional
list_schemas = true  # Optional
list_tables = true  # Optional
list_views = true  # Optional
profile_semi_structured_columns = true  # Optional
sample_table_data = true  # Optional

[execute_query]
# Default timeout_seconds when not specified by the caller (default: 30, must be <= timeout_seconds_max)
timeout_seconds_default = 30  # Optional
# Maximum value accepted by execute_query.timeout_seconds (default: 300, max: 3600)
timeout_seconds_max = 300  # Optional

[analyze_table_statistics]
# Query timeout for analyze_table_statistics (default: 60)
query_timeout_seconds = 60  # Optional (max: 3600)

[profile_semi_structured_columns]
# Query timeouts for semi-structured profiling (default base: 90, path: 180)
base_query_timeout_seconds = 90  # Optional (max: 3600)
path_query_timeout_seconds = 180  # Optional (max: 3600, must be >= base_query_timeout_seconds)
```

`snowflake.secondary_roles` behavior:
- `null` / omitted: Do not execute `USE SECONDARY ROLES`.
- `["NONE"]`: Execute `USE SECONDARY ROLES NONE` for each new session.
- `["ROLE_A", "ROLE_B"]`: Execute `USE SECONDARY ROLES "ROLE_A", "ROLE_B"` for each new session.

### Using Environment Variables

Set the following environment variables:

#### Required
- `SNOWFLAKE__ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE__USER`: Username

#### Optional
- `SNOWFLAKE__PASSWORD`: Password (required when `SNOWFLAKE__AUTHENTICATOR=SNOWFLAKE`)
- `SNOWFLAKE__WAREHOUSE`: Default warehouse
- `SNOWFLAKE__ROLE`: Default role
- `SNOWFLAKE__SECONDARY_ROLES`: JSON array for secondary roles (e.g. `["ROLE_A","ROLE_B"]` or `["NONE"]`)
- `SNOWFLAKE__AUTHENTICATOR`: Authentication method ("SNOWFLAKE" or "externalbrowser")
- `SNOWFLAKE__CLIENT_STORE_TEMPORARY_CREDENTIAL`: Enable ID token cache for browser SSO ("true" or "false", default: "true")

#### Tool Configuration (Optional)
- `TOOLS__ANALYZE_TABLE_STATISTICS`: Enable/disable analyze_table_statistics tool ("true" or "false", default: "true")
- `TOOLS__DESCRIBE_TABLE`: Enable/disable describe_table tool ("true" or "false", default: "true")
- `TOOLS__EXECUTE_QUERY`: Enable/disable execute_query tool ("true" or "false", default: "true")
- `TOOLS__LIST_SCHEMAS`: Enable/disable list_schemas tool ("true" or "false", default: "true")
- `TOOLS__LIST_TABLES`: Enable/disable list_tables tool ("true" or "false", default: "true")
- `TOOLS__LIST_VIEWS`: Enable/disable list_views tool ("true" or "false", default: "true")
- `TOOLS__PROFILE_SEMI_STRUCTURED_COLUMNS`: Enable/disable profile_semi_structured_columns tool ("true" or "false", default: "true")
- `TOOLS__SAMPLE_TABLE_DATA`: Enable/disable sample_table_data tool ("true" or "false", default: "true")
- `EXECUTE_QUERY__TIMEOUT_SECONDS_DEFAULT`: Default `timeout_seconds` for execute_query when not specified by the caller (default: 30, must be <= `timeout_seconds_max`)
- `EXECUTE_QUERY__TIMEOUT_SECONDS_MAX`: Maximum allowed `timeout_seconds` for execute_query (default: 300, max: 3600). Values above 3600 fail server startup.
- `ANALYZE_TABLE_STATISTICS__QUERY_TIMEOUT_SECONDS`: Query timeout for analyze_table_statistics (default: 60, max: 3600)
- `PROFILE_SEMI_STRUCTURED_COLUMNS__BASE_QUERY_TIMEOUT_SECONDS`: Base query timeout for profile_semi_structured_columns (default: 90, max: 3600)
- `PROFILE_SEMI_STRUCTURED_COLUMNS__PATH_QUERY_TIMEOUT_SECONDS`: Path query timeout for profile_semi_structured_columns (default: 180, max: 3600, must be >= base_query_timeout_seconds)

Example:
```bash
export SNOWFLAKE__ACCOUNT="your-account.region"
export SNOWFLAKE__USER="your-username"
export SNOWFLAKE__PASSWORD="your-password"
export SNOWFLAKE__WAREHOUSE="your-warehouse"
export SNOWFLAKE__ROLE="your-role"
export SNOWFLAKE__SECONDARY_ROLES='["ROLE_A","ROLE_B"]'
export SNOWFLAKE__AUTHENTICATOR="SNOWFLAKE"
export SNOWFLAKE__CLIENT_STORE_TEMPORARY_CREDENTIAL="true"
```

For PowerShell (Windows):
```powershell
$env:SNOWFLAKE__ACCOUNT="your-account.region"
$env:SNOWFLAKE__USER="your-username"
$env:SNOWFLAKE__PASSWORD="your-password"
$env:SNOWFLAKE__WAREHOUSE="your-warehouse"
$env:SNOWFLAKE__ROLE="your-role"
$env:SNOWFLAKE__SECONDARY_ROLES='["ROLE_A","ROLE_B"]'
$env:SNOWFLAKE__AUTHENTICATOR="SNOWFLAKE"
$env:SNOWFLAKE__CLIENT_STORE_TEMPORARY_CREDENTIAL="true"

# Tool configuration (optional)
$env:TOOLS__EXECUTE_QUERY="false"  # Disable execute_query tool
$env:TOOLS__ANALYZE_TABLE_STATISTICS="false"  # Disable analyze_table_statistics tool
```

> [!NOTE]
> Environment variables are separated by double underscores (`__`).

### Browser-based SSO (externalbrowser) with ID token cache

To use browser-based SSO, set `authenticator = "externalbrowser"` and keep
`client_store_temporary_credential = true` (default). The connector stores a temporary
ID token in secure local storage and can reuse it for subsequent connections.

```toml
[snowflake]
account = "your-account.region"
user = "your-username"
warehouse = "your-warehouse"
role = "your-role"
secondary_roles = ["analyst_role"]
authenticator = "externalbrowser"
client_store_temporary_credential = true
```

Notes:
- The first connection opens a browser for SSO sign-in.
- Your Snowflake account must have `ALLOW_ID_TOKEN` enabled.
- Install this package with `snowflake-connector-python[secure-local-storage]` support.

## Usage

Start the MCP server:
```bash
uvx mcp-snowflake --config {your-config-path}
```

### Available Tools

#### Tool List
- [`list_schemas`](#list_schemas) - Retrieve a list of schemas from a specified database
- [`list_tables`](#list_tables) - Retrieve a list of tables from a specified database and schema
- [`list_views`](#list_views) - Retrieve a list of views from a specified database and schema
- [`describe_table`](#describe_table) - Retrieve detailed structure information for a specified table
- [`execute_query`](#execute_query) - Execute read-only SQL queries and return structured results
- [`sample_table_data`](#sample_table_data) - Retrieve sample data from a specified table
- [`analyze_table_statistics`](#analyze_table_statistics) - Generate comprehensive statistical analysis for table columns
- [`profile_semi_structured_columns`](#profile_semi_structured_columns) - Profile VARIANT/ARRAY/OBJECT columns and nested paths

#### list_schemas
Retrieve a list of schemas from a specified database.

**Parameters:**
- `database` (string, required): Database name to retrieve schemas from

**Example:**
```json
{
  "name": "list_schemas",
  "arguments": {
    "database": "MY_DATABASE"
  }
}
```

**Response Format:**
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

#### list_tables
Retrieve a list of tables from a specified database and schema.

**Parameters:**
- `database` (string, required): Database name to retrieve tables from
- `schema` (string, required): Schema name to retrieve tables from
- `filter` (object, optional): Name filter
  - `type` (string, required): Filter type (`contains`)
  - `value` (string, required): Substring to match in table names (case-insensitive)

**Example:**
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

**Response Format:**
The list_tables tool returns a compact, token-efficient text format:

```
database: MY_DATABASE
schema: PUBLIC
table_count: 3
tables: CUSTOMERS, ORDERS, PRODUCTS
```

When no tables match, the output is:

```
database: MY_DATABASE
schema: PUBLIC
table_count: 0
tables: (none)
```

#### list_views
Retrieve a list of views from a specified database and schema.

**Parameters:**
- `database` (string, required): Database name to retrieve views from
- `schema` (string, required): Schema name to retrieve views from
- `filter` (object, optional): Name filter
  - `type` (string, required): Filter type (`contains`)
  - `value` (string, required): Substring to match in view names (case-insensitive)

**Example:**
```json
{
  "name": "list_views",
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

**Response Format:**
```
database: MY_DATABASE
schema: PUBLIC
view_count: 3
views: CUSTOMER_VIEW, ORDER_SUMMARY, PRODUCT_CATALOG
```

When no views match:
```
database: MY_DATABASE
schema: PUBLIC
view_count: 0
views: (none)
```

#### describe_table
Retrieve detailed structure information (columns, data types, etc.) for a specified table.

**Parameters:**
- `database` (string, required): Database name containing the table
- `schema` (string, required): Schema name containing the table
- `table` (string, required): Name of the table to describe

**Example:**
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

**Response Format:**
The describe_table tool returns a compact, token-efficient text format:

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

#### execute_query
Execute read-only SQL queries and return structured results. Only SELECT, SHOW, DESCRIBE, EXPLAIN and similar read operations are allowed.

**Parameters:**
- `sql` (string, required): SQL query to execute (read operations only)
- `timeout_seconds` (integer, optional): Query timeout in seconds (default: 30, max: `execute_query.timeout_seconds_max`)

**Example:**
```json
{
  "name": "execute_query",
  "arguments": {
    "sql": "SELECT * FROM customers LIMIT 10",
    "timeout_seconds": 60
  }
}
```

**Response Format:**
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

#### sample_table_data
Retrieve sample data from a specified table using Snowflake's SAMPLE ROW clause for efficient data sampling.

**Parameters:**
- `database` (string, required): Database name containing the table
- `schema` (string, required): Schema name containing the table
- `table` (string, required): Name of the table to sample
- `sample_size` (integer, optional): Number of sample rows to retrieve (default: 10, minimum: 1)
- `columns` (array of strings, optional): List of column names to retrieve (if not specified, all columns will be retrieved)

**Example:**
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

**Response Format:**
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

#### analyze_table_statistics
Analyze table statistics using Snowflake's high-performance approximation functions (APPROX_PERCENTILE, APPROX_TOP_K, APPROX_COUNT_DISTINCT) to efficiently retrieve statistical information for numeric, string, date, and boolean columns.

**Parameters:**
- `database` (string, required): Database name containing the table
- `schema` (string, required): Schema name containing the table
- `table` (string, required): Name of the table to analyze
- `columns` (array of strings, optional): List of column names to analyze (if not specified, all columns will be analyzed)
- `top_k_limit` (integer, optional): Number of top values to retrieve for string columns (default: 10, max: 100)
- `include_null_empty_profile` (boolean, optional): Include per-column quality profile (default: true)
- `include_blank_string_profile` (boolean, optional): Include TRIM-based blank string profile for STRING columns (default: false)

**Example:**
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

**Response Format:**
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

Column types and their fields:
- **Numeric**: count, min, max, avg, percentiles (25th, 50th, 75th), distinct_count_approx
- **String**: count, min_length, max_length, distinct_count_approx, top_values
- **Date**: count, min_date, max_date, date_range_days, distinct_count_approx
- **Boolean**: count, true_count, false_count, true/false percentages (NULL-inclusive and exclusive)
- **quality_profile** (optional): null_count, null_ratio; string adds empty_string_count/ratio and optional blank_string_count/ratio
- **statistics_metadata** (when `include_null_empty_profile` is true): counting mode and approximation notes

**Timeout Configuration:**
- `analyze_table_statistics.query_timeout_seconds` (default: 60)

#### profile_semi_structured_columns
Profile semi-structured columns (`VARIANT`, `ARRAY`, `OBJECT`) using sampled recursive flatten analysis.

**Parameters:**
- `database` (string, required): Database name containing the table
- `schema` (string, required): Schema name containing the table
- `table` (string, required): Name of the table to profile
- `columns` (array of strings, optional): Target columns (empty means auto-select all semi-structured columns)
- `sample_rows` (integer, optional): Sample row count (default: 10000, min: 1, max: 200000)
- `max_depth` (integer, optional): Maximum recursive path depth (default: 4, min: 1, max: 20)
- `top_k_limit` (integer, optional): Top-k limit for frequent values and keys (default: 20, min: 1, max: 100)
- `include_path_stats` (boolean, optional): Include path-level profiling (default: true)
- `include_value_samples` (boolean, optional): Include path-level top_values samples (default: false)

**Example:**
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

**Response Format:**
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

Sections:
- **Header**: database, schema, table, row counts, analyzed column count
- **column_profile**: null ratio, top-level type distribution, optional array_length_stats, optional top_level_keys_top_k
- **path_profile** (when `include_path_stats` is true): path-wise type distribution, distinct count, null ratio, optional top_values
- **unsupported_columns**: columns skipped due to non-semi-structured types
- **warnings**: sampling/depth-limit and approximation notes

**Timeout Configuration:**
- `profile_semi_structured_columns.base_query_timeout_seconds` (default: 90)
- `profile_semi_structured_columns.path_query_timeout_seconds` (default: 180)

## Development

### Development Environment Setup

```bash
uv sync --all-groups --all-packages
```

### Code Formatting

```bash
uv run ruff format .
uv run ruff check --fix .
```

### Code Testing

```bash
uv run pytest --doctest-modules .
```

## Troubleshooting

### Connection Errors
- Verify that configuration file or environment variables are correctly set
- Check that Snowflake account and username are correct
- If `authenticator=SNOWFLAKE`, verify password is set correctly
- Verify network connectivity

### Permission Errors
- Ensure the specified user has permission to access the database
- Set the ROLE if necessary

### Configuration Priority
Settings are loaded in the following order (later settings take precedence):
1. Configuration file (`.mcp_snowflake.toml`)
2. Environment variables

## License

MIT License
