# MCP Snowflake Server

A Model Context Protocol (MCP) server that connects to Snowflake databases and executes SQL queries.

## Features

- **List Schemas**: Retrieve a list of schemas from a specified database
- **List Tables**: Retrieve a list of tables from a specified database and schema
- **List Views**: Retrieve a list of views from a specified database and schema
- **Describe Table**: Retrieve detailed structure information for a specified table

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
The describe_table tool returns a hybrid format combining natural language explanation with structured JSON data for optimal LLM interpretation:

```
Table Schema: MY_DATABASE.PUBLIC.CUSTOMERS

This table has 4 columns with the following structure:

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

**Key characteristics:**
- Primary key: ID
- Required fields: ID, CREATED_AT
- Optional fields: NAME, EMAIL
```

## Development

### Development Environment Setup

```bash
# Create and activate virtual environment
uv venv
source .venv/bin/activate  # Linux/Mac
# or .venv\Scripts\activate  # Windows

# Install development dependencies
uv sync --dev
```

### Code Formatting

```bash
uv run ruff format
uv run ruff check --fix
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
