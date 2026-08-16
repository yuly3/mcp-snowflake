#!/usr/bin/env python3
"""
Snowflake MCP Server

This server provides a Model Context Protocol (MCP) interface to Snowflake.
It allows clients to execute SQL queries against Snowflake and retrieve results.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server, ServerRequestContext
from pydantic_settings import SettingsConfigDict

from .cli import Cli
from .context import ServerContext
from .settings import Settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def handle_list_tools(
    context: ServerRequestContext[ServerContext],
    _params: types.PaginatedRequestParams | None,
) -> types.ListToolsResult:
    """List available tools."""
    return types.ListToolsResult(tools=[tool.definition for tool in context.lifespan_context.tools()])


async def handle_call_tool(
    context: ServerRequestContext[ServerContext],
    params: types.CallToolRequestParams,
) -> types.CallToolResult:
    """Handle tool calls."""
    if tool := context.lifespan_context.tool(params.name):
        content = await tool.perform(params.arguments)
    else:
        content: list[types.ContentBlock] = [types.TextContent(type="text", text=f"Unknown tool: {params.name}")]

    return types.CallToolResult(content=content)


def create_server(settings: Settings) -> Server[ServerContext]:
    """Create an MCP server with application state managed by its lifespan."""

    @asynccontextmanager
    async def lifespan(_: Server[ServerContext]) -> AsyncGenerator[ServerContext]:
        with ThreadPoolExecutor(thread_name_prefix="mcp-snowflake") as executor:
            context = ServerContext(executor, settings)
            logger.info("Snowflake server context initialized successfully")
            yield context

    return Server(
        "mcp-snowflake",
        version="0.1.0",
        lifespan=lifespan,
        on_list_tools=handle_list_tools,
        on_call_tool=handle_call_tool,
    )


async def _async_main() -> None:
    """Run the main entry point for the MCP server."""

    cli = Cli()

    settings_config = SettingsConfigDict(
        env_nested_delimiter="__",
        toml_file=cli.config,
    )
    settings = Settings.build(settings_config)
    server = create_server(settings)

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
