#!/usr/bin/env python3
"""
Snowflake MCP Server

This server provides a Model Context Protocol (MCP) interface to Snowflake.
It allows clients to execute SQL queries against Snowflake and retrieve results.
"""

import asyncio
import logging
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from pydantic_settings import SettingsConfigDict

from .cli import Cli
from .context import ServerContext
from .settings import Settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server = Server("mcp-snowflake")
server_context = ServerContext()


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools."""
    return [tool.definition for tool in server_context.tools()]


@server.call_tool()
async def handle_call_tool(
    name: str,
    arguments: Mapping[str, Any] | None,
) -> Sequence[types.Content]:
    """Handle tool calls."""
    if not server_context.is_available():
        return [
            types.TextContent(
                type="text",
                text="Error: server is not available",
            )
        ]

    if tool := server_context.tool(name):
        return await tool.perform(arguments)

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main() -> None:
    """Run the main entry point for the MCP server."""

    cli = Cli()

    settings_config = SettingsConfigDict(
        env_nested_delimiter="__",
        toml_file=cli.config,
    )
    settings = Settings.build(settings_config)

    with ThreadPoolExecutor(thread_name_prefix="mcp-snowflake") as executor:
        server_context.prepare(
            executor,
            settings.snowflake,
            settings.tools,
            settings.analyze_table_statistics,
            settings.execute_query,
            settings.profile_semi_structured_columns,
        )

        logger.info("Snowflake client initialized successfully")

        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="mcp-snowflake",
                    server_version="0.1.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )


asyncio.run(main())
