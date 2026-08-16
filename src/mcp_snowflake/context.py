"""Server context for managing Snowflake tools and client."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

from cattrs_converter import JsonImmutableConverter

from .adapter import (
    AnalyzeTableStatisticsEffectHandler,
    DescribeTableEffectHandler,
    ExecuteQueryEffectHandler,
    ListDatabasesEffectHandler,
    ListSchemasEffectHandler,
    ListTablesEffectHandler,
    ProfileSemiStructuredColumnsEffectHandler,
    SampleTableDataEffectHandler,
    SearchColumnsEffectHandler,
)
from .settings import Settings
from .snowflake_client import SnowflakeClient
from .tool import (
    AnalyzeTableStatisticsTool,
    DescribeTableTool,
    ExecuteQueryTool,
    ListDatabasesTool,
    ListSchemasTool,
    ListTablesTool,
    ProfileSemiStructuredColumnsTool,
    SampleTableDataTool,
    SearchColumnsTool,
    Tool,
)


class ServerContext:
    """Context for managing Snowflake client and tools."""

    def __init__(self, thread_pool_executor: ThreadPoolExecutor, settings: Settings) -> None:
        """Initialize the server context with its Snowflake client and tools."""
        self._snowflake_client = SnowflakeClient(
            thread_pool_executor,
            settings.snowflake,
        )
        self._json_converter = JsonImmutableConverter()

        all_tools: list[Tool] = [
            AnalyzeTableStatisticsTool(
                AnalyzeTableStatisticsEffectHandler(
                    self._snowflake_client,
                    query_timeout_seconds=settings.analyze_table_statistics.query_timeout_seconds,
                ),
            ),
            DescribeTableTool(
                DescribeTableEffectHandler(
                    self._snowflake_client,
                    query_timeout_seconds=settings.describe_table.query_timeout_seconds,
                )
            ),
            ExecuteQueryTool(
                self._json_converter,
                ExecuteQueryEffectHandler(self._snowflake_client),
                timeout_seconds_default=settings.execute_query.timeout_seconds_default,
                timeout_seconds_max=settings.execute_query.timeout_seconds_max,
            ),
            ListDatabasesTool(
                ListDatabasesEffectHandler(
                    self._snowflake_client,
                    query_timeout_seconds=settings.list_databases.query_timeout_seconds,
                )
            ),
            ListSchemasTool(
                ListSchemasEffectHandler(
                    self._snowflake_client,
                    query_timeout_seconds=settings.list_schemas.query_timeout_seconds,
                )
            ),
            ListTablesTool(
                ListTablesEffectHandler(
                    self._snowflake_client,
                    query_timeout_seconds=settings.list_tables.query_timeout_seconds,
                )
            ),
            ProfileSemiStructuredColumnsTool(
                ProfileSemiStructuredColumnsEffectHandler(
                    self._snowflake_client,
                    base_query_timeout_seconds=settings.profile_semi_structured_columns.base_query_timeout_seconds,
                    path_query_timeout_seconds=settings.profile_semi_structured_columns.path_query_timeout_seconds,
                ),
            ),
            SearchColumnsTool(
                SearchColumnsEffectHandler(
                    self._snowflake_client,
                    query_timeout_seconds=settings.search_columns.query_timeout_seconds,
                )
            ),
            SampleTableDataTool(
                self._json_converter,
                SampleTableDataEffectHandler(
                    self._snowflake_client,
                    query_timeout_seconds=settings.sample_table_data.query_timeout_seconds,
                ),
            ),
        ]

        # Filter tools based on settings
        enabled_tool_names = settings.tools.enabled_tool_names()
        enabled_tools = [tool for tool in all_tools if tool.name in enabled_tool_names]

        self._tools = {tool.name: tool for tool in enabled_tools}

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
