"""Server context for managing Snowflake tools and client."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

from cattrs_converter import JsonImmutableConverter

from .adapter import (
    AnalyzeTableStatisticsEffectHandler,
    DescribeTableEffectHandler,
    ExecuteQueryEffectHandler,
    ListSchemasEffectHandler,
    ListTablesEffectHandler,
    ListViewsEffectHandler,
    SampleTableDataEffectHandler,
)
from .settings import SnowflakeSettings, ToolsSettings
from .snowflake_client import SnowflakeClient
from .tool import (
    AnalyzeTableStatisticsTool,
    DescribeTableTool,
    ExecuteQueryTool,
    ListSchemasTool,
    ListTablesTool,
    ListViewsTool,
    SampleTableDataTool,
    Tool,
)


class ServerContext:
    """Context for managing Snowflake client and tools."""

    def __init__(self) -> None:
        """Initialize the server context with empty state."""
        self._snowflake_client: SnowflakeClient | None = None
        self._json_converter = JsonImmutableConverter()
        self._tools: dict[str, Tool] = {}

    def prepare(
        self,
        thread_pool_executor: ThreadPoolExecutor,
        snowflake_settings: SnowflakeSettings,
        tools_settings: ToolsSettings,
    ) -> None:
        """Prepare the server context with client and tools.

        Parameters
        ----------
        snowflake_client : SnowflakeClient
            The Snowflake client instance to use for database operations.
        tools_settings : ToolsSettings
            Configuration specifying which tools should be enabled.
        """
        self._snowflake_client = SnowflakeClient(
            thread_pool_executor,
            snowflake_settings,
        )

        all_tools: list[Tool] = [
            AnalyzeTableStatisticsTool(
                self._json_converter,
                AnalyzeTableStatisticsEffectHandler(self._snowflake_client),
            ),
            DescribeTableTool(DescribeTableEffectHandler(self._snowflake_client)),
            ExecuteQueryTool(
                self._json_converter,
                ExecuteQueryEffectHandler(self._snowflake_client),
            ),
            ListSchemasTool(ListSchemasEffectHandler(self._snowflake_client)),
            ListTablesTool(ListTablesEffectHandler(self._snowflake_client)),
            ListViewsTool(ListViewsEffectHandler(self._snowflake_client)),
            SampleTableDataTool(
                self._json_converter,
                SampleTableDataEffectHandler(self._snowflake_client),
            ),
        ]

        # Filter tools based on settings
        enabled_tool_names = tools_settings.enabled_tool_names()
        enabled_tools = [tool for tool in all_tools if tool.name in enabled_tool_names]

        self._tools = {tool.name: tool for tool in enabled_tools}

    def is_available(self) -> bool:
        """Check if the context is available for use.

        Returns
        -------
        bool
            True if Snowflake client is initialized, False otherwise.
        """
        return self._snowflake_client is not None

    def tools(self) -> Iterator[Tool]:
        """Get an iterator over all available tools.

        Returns
        -------
        Iterator[Tool]
            Iterator yielding all enabled tools.
        """
        yield from self._tools.values()

    def tool(self, name: str) -> Tool | None:
        """Get a specific tool by name.

        Parameters
        ----------
        name : str
            The name of the tool to retrieve.

        Returns
        -------
        Tool | None
            The tool instance if found, None otherwise.
        """
        return self._tools.get(name)

    def tool_names(self) -> Iterator[str]:
        """Get an iterator over all available tool names.

        Returns
        -------
        Iterator[str]
            Iterator yielding names of all enabled tools.
        """
        yield from self._tools.keys()
