"""Server context for managing Snowflake tools and client."""

from typing import TYPE_CHECKING

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
from .settings import ToolsSettings
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

if TYPE_CHECKING:
    from .snowflake_client import SnowflakeClient


class SnowflakeServerContext:
    """Context class to hold Snowflake client instance."""

    def __init__(self) -> None:
        self.snowflake_client: SnowflakeClient | None = None
        self.json_converter = JsonImmutableConverter()
        self.tools: dict[str, Tool] = {}

    def build_tools(self, tools_settings: ToolsSettings) -> None:
        if not self.snowflake_client:
            raise ValueError("Snowflake client is not initialized")

        all_tools: list[Tool] = [
            AnalyzeTableStatisticsTool(
                self.json_converter,
                AnalyzeTableStatisticsEffectHandler(self.snowflake_client),
            ),
            DescribeTableTool(DescribeTableEffectHandler(self.snowflake_client)),
            ExecuteQueryTool(
                self.json_converter,
                ExecuteQueryEffectHandler(self.snowflake_client),
            ),
            ListSchemasTool(ListSchemasEffectHandler(self.snowflake_client)),
            ListTablesTool(ListTablesEffectHandler(self.snowflake_client)),
            ListViewsTool(ListViewsEffectHandler(self.snowflake_client)),
            SampleTableDataTool(
                self.json_converter,
                SampleTableDataEffectHandler(self.snowflake_client),
            ),
        ]

        # Filter tools based on settings
        enabled_tool_names = tools_settings.enabled_tool_names()
        enabled_tools = [tool for tool in all_tools if tool.name in enabled_tool_names]

        self.tools = {tool.name: tool for tool in enabled_tools}
