"""
Adapter layer for infra implementation.

This layer provides EffectHandler classes that correspond 1:1 with handler Effect protocols.
"""

from .analyze_table_statistics_handler import AnalyzeTableStatisticsEffectHandler
from .describe_table_handler import DescribeTableEffectHandler
from .execute_query_handler import ExecuteQueryEffectHandler
from .list_databases_handler import ListDatabasesEffectHandler
from .list_schemas_handler import ListSchemasEffectHandler
from .list_tables_handler import ListTablesEffectHandler
from .profile_semi_structured_columns_handler import (
    ProfileSemiStructuredColumnsEffectHandler,
)
from .sample_table_data_handler import SampleTableDataEffectHandler
from .search_columns_handler import SearchColumnsEffectHandler

__all__ = [
    "AnalyzeTableStatisticsEffectHandler",
    "DescribeTableEffectHandler",
    "ExecuteQueryEffectHandler",
    "ListDatabasesEffectHandler",
    "ListSchemasEffectHandler",
    "ListTablesEffectHandler",
    "ProfileSemiStructuredColumnsEffectHandler",
    "SampleTableDataEffectHandler",
    "SearchColumnsEffectHandler",
]
