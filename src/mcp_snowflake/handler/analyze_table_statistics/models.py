"""Public API models for table statistics analysis."""

from datetime import timedelta
from typing import Any, Protocol

from pydantic import BaseModel, Field


class AnalyzeTableStatisticsArgs(BaseModel):
    """Arguments for analyzing table statistics."""

    database: str
    schema_name: str
    table_name: str
    columns: list[str] = Field(default_factory=list)  # 空リストの場合は全列
    top_k_limit: int = Field(default=10, ge=1, le=100)  # 最頻値の取得数


class EffectAnalyzeTableStatistics(Protocol):
    """Protocol for dependencies required by table statistics analysis."""

    async def describe_table(
        self,
        database: str,
        schema: str,
        table_name: str,
    ) -> dict[str, Any]: ...

    async def execute_query(
        self,
        query: str,
        query_timeout: timedelta | None = None,
    ) -> list[dict[str, Any]]: ...
