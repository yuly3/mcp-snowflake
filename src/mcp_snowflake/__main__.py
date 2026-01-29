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

from cattrs_converter import JsonImmutableConverter

from .adapter import (
    AnalyzeTableStatisticsEffectHandler,
    DescribeTableEffectHandler,
    ExecuteQueryEffectHandler,
    ListRolesEffectHandler,
    ListSchemasEffectHandler,
    ListTablesEffectHandler,
    ListViewsEffectHandler,
    ListWarehousesEffectHandler,
    SampleTableDataEffectHandler,
)
from .cli import Cli
from .settings import Settings
from .snowflake_client import SnowflakeClient
from .tool import (
    AnalyzeTableStatisticsTool,
    DescribeTableTool,
    ExecuteQueryTool,
    ListRolesTool,
    ListSchemasTool,
    ListTablesTool,
    ListViewsTool,
    ListWarehousesTool,
    SampleTableDataTool,
    Tool,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a server instance
server = Server("mcp-snowflake")


class SnowflakeServerContext:
    """Context class to hold Snowflake client instance."""

    def __init__(self) -> None:
        self.snowflake_client: SnowflakeClient | None = None
        self.json_converter = JsonImmutableConverter()
        self.tools: Mapping[str, Tool] = {}

    def build_tools(self) -> None:
        if not self.snowflake_client:
            raise ValueError("Snowflake client is not initialized")

        tools = [
            AnalyzeTableStatisticsTool(
                self.json_converter,
                AnalyzeTableStatisticsEffectHandler(self.snowflake_client),
            ),
            DescribeTableTool(DescribeTableEffectHandler(self.snowflake_client)),
            ExecuteQueryTool(
                self.json_converter,
                ExecuteQueryEffectHandler(self.snowflake_client),
            ),
            ListRolesTool(ListRolesEffectHandler(self.snowflake_client)),
            ListSchemasTool(ListSchemasEffectHandler(self.snowflake_client)),
            ListTablesTool(ListTablesEffectHandler(self.snowflake_client)),
            ListViewsTool(ListViewsEffectHandler(self.snowflake_client)),
            ListWarehousesTool(ListWarehousesEffectHandler(self.snowflake_client)),
            SampleTableDataTool(
                self.json_converter,
                SampleTableDataEffectHandler(self.snowflake_client),
            ),
        ]
        self.tools = {tool.name: tool for tool in tools}


server_context = SnowflakeServerContext()


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools."""
    return [tool.definition for tool in server_context.tools.values()]


@server.call_tool()
async def handle_call_tool(
    name: str,
    arguments: Mapping[str, Any] | None,
) -> Sequence[types.Content]:
    """Handle tool calls."""
    if not server_context.snowflake_client:
        return [
            types.TextContent(
                type="text",
                text="Error: Snowflake client is not initialized",
            )
        ]

    if tool := server_context.tools.get(name):
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
        server_context.snowflake_client = SnowflakeClient(
            executor,
            settings.snowflake,
        )

        logger.info("Snowflake client initialized successfully")

        server_context.build_tools()
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
