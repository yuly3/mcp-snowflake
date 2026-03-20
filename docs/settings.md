# Settings

## Using Configuration File (Recommended)

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
profile_semi_structured_columns = true  # Optional
sample_table_data = true  # Optional
search_columns = true  # Optional

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

## Using Environment Variables

Set the following environment variables:

### Required
- `SNOWFLAKE__ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE__USER`: Username

### Optional
- `SNOWFLAKE__PASSWORD`: Password (required when `SNOWFLAKE__AUTHENTICATOR=SNOWFLAKE`)
- `SNOWFLAKE__WAREHOUSE`: Default warehouse
- `SNOWFLAKE__ROLE`: Default role
- `SNOWFLAKE__SECONDARY_ROLES`: JSON array for secondary roles (e.g. `["ROLE_A","ROLE_B"]` or `["NONE"]`)
- `SNOWFLAKE__AUTHENTICATOR`: Authentication method ("SNOWFLAKE" or "externalbrowser")
- `SNOWFLAKE__CLIENT_STORE_TEMPORARY_CREDENTIAL`: Enable ID token cache for browser SSO ("true" or "false", default: "true")

### Tool Configuration (Optional)
- `TOOLS__ANALYZE_TABLE_STATISTICS`: Enable/disable analyze_table_statistics tool ("true" or "false", default: "true")
- `TOOLS__DESCRIBE_TABLE`: Enable/disable describe_table tool ("true" or "false", default: "true")
- `TOOLS__EXECUTE_QUERY`: Enable/disable execute_query tool ("true" or "false", default: "true")
- `TOOLS__LIST_SCHEMAS`: Enable/disable list_schemas tool ("true" or "false", default: "true")
- `TOOLS__LIST_TABLES`: Enable/disable list_tables tool ("true" or "false", default: "true")
- `TOOLS__PROFILE_SEMI_STRUCTURED_COLUMNS`: Enable/disable profile_semi_structured_columns tool ("true" or "false", default: "true")
- `TOOLS__SAMPLE_TABLE_DATA`: Enable/disable sample_table_data tool ("true" or "false", default: "true")
- `TOOLS__SEARCH_COLUMNS`: Enable/disable search_columns tool ("true" or "false", default: "true")
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

### Configuration Priority
Settings are loaded in the following order (later settings take precedence):
1. Configuration file (`.mcp_snowflake.toml`)
2. Environment variables

## Browser-based SSO (externalbrowser) with ID token cache

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
