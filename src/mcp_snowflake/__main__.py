#!/usr/bin/env python3
"""
Snowflake MCP Server

This server provides a Model Context Protocol (MCP) interface to Snowflake.
It allows clients to execute SQL queries against Snowflake and retrieve results.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from .cli import Cli
from .handler import (
    ListSchemasArgs,
    ListTablesArgs,
    handle_list_schemas,
    handle_list_tables,
)
from .settings import Settings
from .snowflake_client import SnowflakeClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a server instance
server = Server("mcp-snowflake")


class SnowflakeServerContext:
    """Context class to hold Snowflake client instance."""

    def __init__(self) -> None:
        self.snowflake_client: SnowflakeClient | None = None


server_context = SnowflakeServerContext()


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools."""
    return [
        types.Tool(
            name="list_schemas",
            description="Retrieve a list of schemas from a specified database",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to retrieve schemas from",
                    }
                },
                "required": ["database"],
            },
        ),
        types.Tool(
            name="list_tables",
            description="Retrieve a list of tables from a specified database and schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to retrieve tables from",
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Schema name to retrieve tables from",
                    },
                },
                "required": ["database", "schema_name"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str,
    arguments: dict[str, Any] | None,
) -> list[types.TextContent]:
    """Handle tool calls."""
    if not server_context.snowflake_client:
        return [
            types.TextContent(
                type="text",
                text="Error: Snowflake client is not initialized",
            )
        ]

    if name == "list_schemas":
        try:
            args = ListSchemasArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for list_schemas: {e}",
                )
            ]
        return await handle_list_schemas(args, server_context.snowflake_client)

    if name == "list_tables":
        try:
            args = ListTablesArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for list_tables: {e}",
                )
            ]
        return await handle_list_tables(args, server_context.snowflake_client)

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main() -> None:
    """Main entry point for the MCP server."""

    cli = Cli()

    settings_config = SettingsConfigDict(
        env_nested_delimiter="__",
        toml_file=cli.config,
    )
    settings = Settings.build(settings_config)

    with ThreadPoolExecutor(thread_name_prefix="mcp-snowflake") as executor:
        try:
            server_context.snowflake_client = SnowflakeClient(
                executor,
                settings.snowflake,
            )
        except Exception as e:
            logger.exception(f"Failed to initialize Snowflake client: {e}")
            raise

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
