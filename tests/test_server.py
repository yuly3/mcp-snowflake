from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import mcp.types as types
import pytest
from mcp.server import ServerRequestContext
from pydantic_settings import SettingsConfigDict

from mcp_snowflake.__main__ import create_server, handle_call_tool, handle_list_tools
from mcp_snowflake.context import ServerContext
from mcp_snowflake.settings import Settings

if TYPE_CHECKING:
    from mcp.server.session import ServerSession


@pytest.fixture
def settings() -> Settings:
    config_path = Path(__file__).parent / "fixtures" / "test.mcp_snowflake.toml"
    return Settings.build(SettingsConfigDict(toml_file=str(config_path)))


@pytest.mark.asyncio
async def test_handlers_use_server_context_from_lifespan(settings: Settings) -> None:
    server = create_server(settings)

    async with server.lifespan(server) as server_context:
        assert isinstance(server_context, ServerContext)
        request_context = ServerRequestContext(
            session=cast("ServerSession", Mock()),
            lifespan_context=server_context,
            protocol_version="2025-06-18",
            method="tools/list",
        )

        list_result = await handle_list_tools(request_context, None)
        listed_tool_names = {tool.name for tool in list_result.tools}
        assert "list_tables" not in listed_tool_names

        call_result = await handle_call_tool(
            request_context,
            types.CallToolRequestParams(name="list_tables"),
        )
        assert isinstance(call_result, types.CallToolResult)
        assert call_result.content == [types.TextContent(type="text", text="Unknown tool: list_tables")]
