# MCP Snowflake Server

A Model Context Protocol (MCP) server that connects to Snowflake databases and executes SQL queries.

## Features

- [`list_schemas`](docs/tools/list_schemas.md) - Retrieve a list of schemas from a specified database
- [`list_tables`](docs/tools/list_tables.md) - Retrieve a list of tables and views from a specified database and schema (supports filtering by name or object type)
- [`describe_table`](docs/tools/describe_table.md) - Retrieve detailed structure information for a specified table
- [`execute_query`](docs/tools/execute_query.md) - Execute read-only SQL queries and return results
- [`sample_table_data`](docs/tools/sample_table_data.md) - Retrieve sample data from a specified table using Snowflake's SAMPLE ROW clause
- [`analyze_table_statistics`](docs/tools/analyze_table_statistics.md) - Generate comprehensive statistical analysis for table columns using approximation functions
- [`profile_semi_structured_columns`](docs/tools/profile_semi_structured_columns.md) - Profile VARIANT/ARRAY/OBJECT columns with sampled flatten-based analysis

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

Copy the configuration file sample and edit it:

   ```bash
   cp .mcp_snowflake.toml.example .mcp_snowflake.toml
   ```

```toml
[snowflake]
account = "your-account.region"
user = "your-username"
password = "your-password"
warehouse = "your-warehouse"
role = "your-role"
authenticator = "SNOWFLAKE"
```

Environment variables (`SNOWFLAKE__ACCOUNT`, `SNOWFLAKE__USER`, etc.) are also supported.

For full configuration reference including tool toggles, timeout tuning, secondary roles, and browser-based SSO, see [docs/settings.md](docs/settings.md).

## Usage

Start the MCP server:
```bash
uvx mcp-snowflake --config {your-config-path}
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

## Development

See [docs/development.md](docs/development.md).

## License

MIT License
