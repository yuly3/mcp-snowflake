"""Test for tool registration filtering based on settings."""

import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic_settings import SettingsConfigDict

from mcp_snowflake.context import ServerContext
from mcp_snowflake.settings import Settings, SnowflakeSettings
from mcp_snowflake.snowflake_client import SnowflakeClient


@pytest.fixture
def mock_snowflake_client() -> SnowflakeClient:
    """Create a mock SnowflakeClient for testing."""
    mock_executor = Mock(spec=ThreadPoolExecutor)
    mock_settings = Mock(spec=SnowflakeSettings)
    return SnowflakeClient(mock_executor, mock_settings)


@pytest.fixture
def base_settings() -> Settings:
    """Create base Settings with valid snowflake config."""
    # Use Settings.build() to properly initialize including default_factory
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"  # nosec
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        return Settings.build(SettingsConfigDict(toml_file=temp_file))
    finally:
        Path(temp_file).unlink()


def test_build_tools_respects_settings(
    mock_snowflake_client: SnowflakeClient,
    base_settings: Settings,
) -> None:
    """Test that build_tools only registers enabled tools."""
    # Override tools settings to have some disabled
    base_settings.tools.describe_table = False  # Disabled
    base_settings.tools.list_schemas = False  # Disabled
    base_settings.tools.sample_table_data = False  # Disabled

    server_context = ServerContext()
    server_context.prepare(
        mock_snowflake_client,
        base_settings.tools,
    )

    registered_tool_names = set(server_context.tool_names())
    expected_enabled = {
        "analyze_table_statistics",
        "execute_query",
        "list_tables",
        "list_views",
    }

    assert registered_tool_names == expected_enabled

    # Verify disabled tools are not present
    disabled_tools = {"describe_table", "list_schemas", "sample_table_data"}
    assert registered_tool_names.isdisjoint(disabled_tools)
