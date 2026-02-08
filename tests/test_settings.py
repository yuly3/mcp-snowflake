import os
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from mcp_snowflake.settings import Settings, ToolsSettings


@pytest.fixture
def config_path() -> Path:
    return Path(__file__).parent / "fixtures" / "test.mcp_snowflake.toml"


def test_settings(config_path: Path) -> None:
    settings = Settings.build(SettingsConfigDict(toml_file=config_path))
    assert settings.snowflake.password is not None
    assert settings.snowflake.account == "dummy"
    assert settings.snowflake.role == "dummy"
    assert settings.snowflake.warehouse == "dummy"
    assert settings.snowflake.user == "dummy"
    assert settings.snowflake.password.get_secret_value() == "dummy"
    assert settings.snowflake.authenticator == "SNOWFLAKE"
    assert settings.snowflake.client_store_temporary_credential is True
    assert settings.snowflake.secondary_roles is None
    assert settings.analyze_table_statistics.query_timeout_seconds == 60
    assert settings.execute_query.timeout_seconds_max == 300
    assert settings.profile_semi_structured_columns.base_query_timeout_seconds == 90
    assert settings.profile_semi_structured_columns.path_query_timeout_seconds == 180


def test_settings_externalbrowser_without_password() -> None:
    """Test externalbrowser auth can be configured without password."""
    toml_content = """
[snowflake]
account = "test-account"
role = "test-role"
warehouse = "test-warehouse"
user = "test-user"
authenticator = "externalbrowser"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file))
        assert settings.snowflake.authenticator == "externalbrowser"
        assert settings.snowflake.password is None
        assert settings.snowflake.client_store_temporary_credential is True
    finally:
        Path(temp_file).unlink()


def test_settings_snowflake_auth_requires_password() -> None:
    """Test SNOWFLAKE auth requires password."""
    toml_content = """
[snowflake]
account = "test-account"
role = "test-role"
warehouse = "test-warehouse"
user = "test-user"
authenticator = "SNOWFLAKE"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        with pytest.raises(ValidationError, match="password is required"):
            _ = Settings.build(SettingsConfigDict(toml_file=temp_file))
    finally:
        Path(temp_file).unlink()


def test_settings_secondary_roles_toml_array() -> None:
    """Test secondary roles can be configured as role name array."""
    toml_content = """
[snowflake]
account = "test-account"
role = "test-role"
warehouse = "test-warehouse"
user = "test-user"
password = "test-password"
secondary_roles = ["role_a", "role_b"]
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file))
        assert settings.snowflake.secondary_roles == ["role_a", "role_b"]
    finally:
        Path(temp_file).unlink()


def test_settings_secondary_roles_none_keyword() -> None:
    """Test secondary roles NONE keyword can be configured explicitly."""
    toml_content = """
[snowflake]
account = "test-account"
role = "test-role"
warehouse = "test-warehouse"
user = "test-user"
password = "test-password"
secondary_roles = ["NONE"]
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file))
        assert settings.snowflake.secondary_roles == ["NONE"]
    finally:
        Path(temp_file).unlink()


def test_settings_secondary_roles_empty_fails() -> None:
    """Test empty secondary roles list fails validation."""
    toml_content = """
[snowflake]
account = "test-account"
role = "test-role"
warehouse = "test-warehouse"
user = "test-user"
password = "test-password"
secondary_roles = []
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        with pytest.raises(ValidationError, match="must not be empty"):
            _ = Settings.build(SettingsConfigDict(toml_file=temp_file))
    finally:
        Path(temp_file).unlink()


def test_settings_secondary_roles_none_with_role_names_fails() -> None:
    """Test NONE keyword cannot be mixed with role names."""
    toml_content = """
[snowflake]
account = "test-account"
role = "test-role"
warehouse = "test-warehouse"
user = "test-user"
password = "test-password"
secondary_roles = ["NONE", "role_a"]
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        with pytest.raises(ValidationError, match="exactly one value when using ALL or NONE"):
            _ = Settings.build(SettingsConfigDict(toml_file=temp_file))
    finally:
        Path(temp_file).unlink()


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
        assert settings.tools.profile_semi_structured_columns is True
        assert settings.tools.sample_table_data is True
    finally:
        Path(temp_file).unlink()


def test_execute_query_timeout_max_toml_override() -> None:
    """Test that TOML config can override execute_query timeout max."""
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"

[execute_query]
timeout_seconds_max = 1800
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file))
        assert settings.execute_query.timeout_seconds_max == 1800
    finally:
        Path(temp_file).unlink()


def test_analyze_table_statistics_timeout_toml_override() -> None:
    """Test that TOML config can override analyze_table_statistics timeout."""
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"

[analyze_table_statistics]
query_timeout_seconds = 120
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file))
        assert settings.analyze_table_statistics.query_timeout_seconds == 120
    finally:
        Path(temp_file).unlink()


def test_analyze_table_statistics_timeout_greater_than_one_hour_fails() -> None:
    """Test that analyze_table_statistics timeout above one hour fails validation."""
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"

[analyze_table_statistics]
query_timeout_seconds = 3601
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        with pytest.raises(ValidationError, match="less than or equal to 3600"):
            _ = Settings.build(SettingsConfigDict(toml_file=temp_file))
    finally:
        Path(temp_file).unlink()


def test_execute_query_timeout_max_greater_than_one_hour_fails() -> None:
    """Test that timeout max above one hour fails validation at startup."""
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"

[execute_query]
timeout_seconds_max = 3601
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        with pytest.raises(ValidationError, match="less than or equal to 3600"):
            _ = Settings.build(SettingsConfigDict(toml_file=temp_file))
    finally:
        Path(temp_file).unlink()


def test_profile_timeout_toml_override() -> None:
    """Test that TOML config can override profile tool timeouts."""
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"

[profile_semi_structured_columns]
base_query_timeout_seconds = 120
path_query_timeout_seconds = 300
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        settings = Settings.build(SettingsConfigDict(toml_file=temp_file))
        assert settings.profile_semi_structured_columns.base_query_timeout_seconds == 120
        assert settings.profile_semi_structured_columns.path_query_timeout_seconds == 300
    finally:
        Path(temp_file).unlink()


def test_profile_timeout_path_shorter_than_base_fails() -> None:
    """Test that path timeout cannot be shorter than base timeout."""
    toml_content = """
[snowflake]
account = "test"
role = "test"
warehouse = "test"
user = "test"
password = "test"

[profile_semi_structured_columns]
base_query_timeout_seconds = 300
path_query_timeout_seconds = 120
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        _ = f.write(toml_content)
        temp_file = f.name

    try:
        with pytest.raises(ValidationError, match="must be greater than or equal to"):
            _ = Settings.build(SettingsConfigDict(toml_file=temp_file))
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
        "profile_semi_structured_columns",
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
    tools_settings.profile_semi_structured_columns = False

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
    tools_settings.profile_semi_structured_columns = False
    tools_settings.sample_table_data = False

    assert tools_settings.enabled_tool_names() == set()
