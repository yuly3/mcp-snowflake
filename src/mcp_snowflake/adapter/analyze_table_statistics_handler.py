"""AnalyzeTableStatistics EffectHandler implementation."""

from ..snowflake_client import SnowflakeClient
from .describe_table_handler import DescribeTableEffectHandler
from .execute_query_handler import ExecuteQueryEffectHandler


class AnalyzeTableStatisticsEffectHandler(
    DescribeTableEffectHandler,
    ExecuteQueryEffectHandler,
):
    """EffectHandler for AnalyzeTableStatistics operations.

    Inherits from both DescribeTableEffectHandler and ExecuteQueryEffectHandler
    to satisfy the EffectAnalyzeTableStatistics protocol.
    """

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient.

        Uses consistent initialization for multiple inheritance.
        """
        # Both parent classes have the same __init__ signature, so this works
        super().__init__(client)
