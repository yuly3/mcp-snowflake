"""Public API models for table statistics analysis."""

from typing import Protocol

from pydantic import BaseModel, Field

from ..describe_table import EffectDescribeTable
from ..execute_query import EffectExecuteQuery


class AnalyzeTableStatisticsArgs(BaseModel):
    """Arguments for analyzing table statistics."""

    database: str
    schema_name: str
    table_name: str
    columns: list[str] = Field(default_factory=list)  # 空リストの場合は全列
    top_k_limit: int = Field(default=10, ge=1, le=100)  # 最頻値の取得数


class EffectAnalyzeTableStatistics(EffectDescribeTable, EffectExecuteQuery, Protocol):
    """Protocol for dependencies required by table statistics analysis."""
