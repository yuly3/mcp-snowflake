import os
import tempfile
from pathlib import Path

import pytest
from pydantic_settings import SettingsConfigDict

from mcp_snowflake.settings import Settings, ToolsSettings


@pytest.fixture
def config_path() -> Path:
    return Path(__file__).parent / "fixtures" / "test.mcp_snowflake.toml"


def test_settings(config_path: Path) -> None:
    settings = Settings.build(SettingsConfigDict(toml_file=config_path))
    assert settings.snowflake.account == "dummy"
    assert settings.snowflake.role == "dummy"
    assert settings.snowflake.warehouse == "dummy"
    assert settings.snowflake.user == "dummy"
    assert settings.snowflake.password.get_secret_value() == "dummy"


def test_tools_default_all_enabled() -> None:
    """Test that all tools are enabled by default when not specified in config."""
    # Create a minimal valid TOML content for testing tools only

    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file))

        # All tools should be enabled by default
        assert settings.tools.analyze_table_statistics is True
        assert settings.tools.describe_table is True
        assert settings.tools.execute_query is True
        assert settings.tools.list_schemas is True
        assert settings.tools.list_tables is True
        assert settings.tools.list_views is True
        assert settings.tools.sample_table_data is True
    finally:
        Path(temp_file).unlink()


def test_tools_toml_override(config_path: Path) -> None:
    """Test that TOML config can override tool settings."""
    settings = Settings.build(SettingsConfigDict(toml_file=config_path))

    # Check that TOML overrides work (list_tables = false in fixture)
    assert settings.tools.list_tables is False
    # Others should remain default (True)
    assert settings.tools.execute_query is True
    assert settings.tools.analyze_table_statistics is True


def test_tools_env_override() -> None:
    """Test that environment variables can override tool settings."""

    # Create a minimal valid TOML content
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"
"""

    os.environ["TOOLS__LIST_TABLES"] = "false"
    os.environ["TOOLS__EXECUTE_QUERY"] = "true"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file, env_nested_delimiter="__"))

        # Check that environment variables override defaults
        assert settings.tools.list_tables is False
        assert settings.tools.execute_query is True
        # Others should remain default (True)
        assert settings.tools.analyze_table_statistics is True
        assert settings.tools.describe_table is True
    finally:
        # Clean up environment variables and temp file
        _ = os.environ.pop("TOOLS__LIST_TABLES", None)
        _ = os.environ.pop("TOOLS__EXECUTE_QUERY", None)

        Path(temp_file).unlink()


def test_enabled_tool_names_default() -> None:
    """Test that enabled_tool_names returns all tools when all are enabled (default)."""
    tools_settings = ToolsSettings()

    expected_all_tools = {
        "analyze_table_statistics",
        "describe_table",
        "execute_query",
        "list_schemas",
        "list_tables",
        "list_views",
        "sample_table_data",
    }

    assert tools_settings.enabled_tool_names() == expected_all_tools


def test_enabled_tool_names_partial() -> None:
    """Test that enabled_tool_names returns only enabled tools when some are disabled."""
    tools_settings = ToolsSettings()

    # Disable some tools
    tools_settings.describe_table = False
    tools_settings.execute_query = False
    tools_settings.sample_table_data = False

    expected_enabled = {
        "analyze_table_statistics",
        "list_schemas",
        "list_tables",
        "list_views",
    }

    assert tools_settings.enabled_tool_names() == expected_enabled


def test_enabled_tool_names_all_disabled() -> None:
    """Test that enabled_tool_names returns empty set when all tools are disabled."""
    tools_settings = ToolsSettings()

    # Disable all tools
    tools_settings.analyze_table_statistics = False
    tools_settings.describe_table = False
    tools_settings.execute_query = False
    tools_settings.list_schemas = False
    tools_settings.list_tables = False
    tools_settings.list_views = False
    tools_settings.sample_table_data = False

    assert tools_settings.enabled_tool_names() == set()
