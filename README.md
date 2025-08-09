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
password = "your-password"
warehouse = "your-warehouse"  # Optional
role = "your-role"  # Optional
authenticator = "SNOWFLAKE"  # "SNOWFLAKE" or "externalbrowser"
```

### Using Environment Variables

Set the following environment variables:

#### Required
- `SNOWFLAKE__ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE__USER`: Username
- `SNOWFLAKE__PASSWORD`: Password

#### Optional
- `SNOWFLAKE__WAREHOUSE`: Default warehouse
- `SNOWFLAKE__ROLE`: Default role
- `SNOWFLAKE__AUTHENTICATOR`: Authentication method ("SNOWFLAKE" or "externalbrowser")

Example:
```bash
export SNOWFLAKE__ACCOUNT="your-account.region"
export SNOWFLAKE__USER="your-username"
export SNOWFLAKE__PASSWORD="your-password"
export SNOWFLAKE__WAREHOUSE="your-warehouse"
export SNOWFLAKE__ROLE="your-role"
export SNOWFLAKE__AUTHENTICATOR="SNOWFLAKE"
```

> [!NOTE]
> Environment variables are separated by double underscores (`__`).

## Usage

Start the MCP server:
```bash
uvx mcp-snowflake --config {your-config-path}
```

### Available Tools

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

#### list_tables
Retrieve a list of tables from a specified database and schema.

**Parameters:**
- `database` (string, required): Database name to retrieve tables from
- `schema_name` (string, required): Schema name to retrieve tables from

**Example:**
```json
{
  "name": "list_tables",
  "arguments": {
    "database": "MY_DATABASE",
    "schema_name": "PUBLIC"
  }
}
```

#### list_views
Retrieve a list of views from a specified database and schema.

**Parameters:**
- `database` (string, required): Database name to retrieve views from
- `schema_name` (string, required): Schema name to retrieve views from

**Example:**
```json
{
  "name": "list_views",
  "arguments": {
    "database": "MY_DATABASE",
    "schema_name": "PUBLIC"
  }
}
```

#### describe_table
Retrieve detailed structure information (columns, data types, etc.) for a specified table.

**Parameters:**
- `database` (string, required): Database name containing the table
- `schema_name` (string, required): Schema name containing the table
- `table_name` (string, required): Name of the table to describe

**Example:**
```json
{
  "name": "describe_table",
  "arguments": {
    "database": "MY_DATABASE",
    "schema_name": "PUBLIC",
    "table_name": "CUSTOMERS"
  }
}
```

**Response Format:**
The describe_table tool returns a structured JSON format:

```json
{
  "table_info": {
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "name": "CUSTOMERS",
    "column_count": 4,
    "columns": [
      {
        "name": "ID",
        "data_type": "NUMBER(38,0)",
        "nullable": false,
        "default_value": null,
        "comment": "Primary key",
        "ordinal_position": 1
      }
    ]
  }
}
```

#### execute_query
Execute read-only SQL queries and return structured results. Only SELECT, SHOW, DESCRIBE, EXPLAIN and similar read operations are allowed.

**Parameters:**
- `sql` (string, required): SQL query to execute (read operations only)
- `timeout_seconds` (integer, optional): Query timeout in seconds (default: 30, max: 300)

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
```json
{
  "execution_time_ms": 150,
  "row_count": 10,
  "columns": ["id", "name", "email"],
  "rows": [
    {"id": 1, "name": "John", "email": "john@example.com"}
  ],
  "warnings": []
}
```

#### sample_table_data
Retrieve sample data from a specified table using Snowflake's SAMPLE ROW clause for efficient data sampling.

**Parameters:**
- `database` (string, required): Database name containing the table
- `schema_name` (string, required): Schema name containing the table
- `table_name` (string, required): Name of the table to sample
- `sample_size` (integer, optional): Number of sample rows to retrieve (default: 10, minimum: 1)
- `columns` (array of strings, optional): List of column names to retrieve (if not specified, all columns will be retrieved)

**Example:**
```json
{
  "name": "sample_table_data",
  "arguments": {
    "database": "MY_DATABASE",
    "schema_name": "PUBLIC",
    "table_name": "ORDERS",
    "sample_size": 5,
    "columns": ["order_id", "customer_id", "total"]
  }
}
```

#### analyze_table_statistics
Analyze table statistics using Snowflake's high-performance approximation functions (APPROX_PERCENTILE, APPROX_TOP_K, APPROX_COUNT_DISTINCT) to efficiently retrieve statistical information for numeric, string, date, and boolean columns.

**Parameters:**
- `database` (string, required): Database name containing the table
- `schema_name` (string, required): Schema name containing the table
- `table_name` (string, required): Name of the table to analyze
- `columns` (array of strings, optional): List of column names to analyze (if not specified, all columns will be analyzed)
- `top_k_limit` (integer, optional): Number of top values to retrieve for string columns (default: 10, max: 100)

**Example:**
```json
{
  "name": "analyze_table_statistics",
  "arguments": {
    "database": "MY_DATABASE",
    "schema_name": "PUBLIC",
    "table_name": "SALES_DATA",
    "columns": ["amount", "region", "order_date"],
    "top_k_limit": 5
  }
}
```

**Response Format:**
Returns comprehensive statistics tailored to each column type:
- **Numeric columns**: count, min, max, avg, percentiles (25th, 50th, 75th), distinct count
- **String columns**: count, min/max length, distinct count, top K most frequent values
- **Date columns**: count, min/max dates, date range in days, distinct count
- **Boolean columns**: count, true/false counts and percentages (both NULL-inclusive and NULL-exclusive)

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
- Check that Snowflake account, username, and password are correct
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
