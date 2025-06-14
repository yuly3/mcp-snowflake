import sys
from pathlib import Path

import pytest

from mcp_snowflake.cli import Cli


@pytest.fixture
def config_path() -> str:
    return str(Path(__file__).parent / "fixtures" / "test.mcp_snowflake.toml")


def test_cli(config_path: str) -> None:
    sys.argv = ["test.py", "--config", config_path]
    cli = Cli()
    assert cli.config == Path(__file__).parent / "fixtures" / "test.mcp_snowflake.toml"
