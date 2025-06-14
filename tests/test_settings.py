from pathlib import Path

import pytest
from pydantic_settings import SettingsConfigDict

from mcp_snowflake.settings import Settings


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
