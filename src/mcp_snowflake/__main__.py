#!/usr/bin/env python3
"""
Snowflake MCP Server

This server provides a Model Context Protocol (MCP) interface to Snowflake.
It allows clients to execute SQL queries against Snowflake and retrieve results.
"""

import asyncio
import logging
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from pydantic_settings import SettingsConfigDict

from .cli import Cli
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


# Server context
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
        )
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
        if not arguments or "database" not in arguments:
            return [
                types.TextContent(
                    type="text",
                    text="Error: Database name is not specified",
                )
            ]

        database = arguments["database"]
        try:
            schemas = await server_context.snowflake_client.list_schemas(database)
        except Exception as e:
            logger.exception(f"Error listing schemas: {e}")
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Failed to retrieve schemas: {e!s}",
                )
            ]

        schema_list = "\n".join([f"- {schema}" for schema in schemas])
        return [
            types.TextContent(
                type="text",
                text=f"Schema list for database '{database}':\n{schema_list}",
            )
        ]

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main() -> None:
    """Main entry point for the MCP server."""

    cli = Cli()

    settings_config = SettingsConfigDict(
        env_nested_delimiter="__",
        toml_file=cli.config,
    )
    settings = Settings.build(settings_config)

    try:
        server_context.snowflake_client = SnowflakeClient(settings.snowflake)
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
