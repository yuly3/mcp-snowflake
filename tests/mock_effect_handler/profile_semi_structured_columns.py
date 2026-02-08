"""Mock implementation of EffectProfileSemiStructuredColumns protocol."""

from kernel.table_metadata import DataBase, Schema, Table, TableColumn, TableInfo
from mcp_snowflake.handler.profile_semi_structured_columns.models import (
    SemiStructuredProfileParseResult,
)


class MockProfileSemiStructuredColumns:
    """Mock implementation of EffectProfileSemiStructuredColumns protocol."""

    def __init__(
        self,
        table_info: TableInfo | None = None,
        profile_result: SemiStructuredProfileParseResult | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.table_info = table_info
        self.profile_result = profile_result
        self.should_raise = should_raise

    async def describe_table(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table: str,  # noqa: ARG002
    ) -> TableInfo:
        """Mock describe_table implementation."""
        if self.should_raise:
            raise self.should_raise
        if self.table_info is None:
            return TableInfo(
                database=DataBase("default_db"),
                schema=Schema("default_schema"),
                name="default_table",
                column_count=0,
                columns=[],
            )
        return self.table_info

    async def profile_semi_structured_columns(
        self,
        database: DataBase,  # noqa: ARG002
        schema: Schema,  # noqa: ARG002
        table: Table,  # noqa: ARG002
        columns_to_profile: list[TableColumn],  # noqa: ARG002
        sample_rows: int,  # noqa: ARG002
        max_depth: int,  # noqa: ARG002
        top_k_limit: int,  # noqa: ARG002
        include_path_stats: bool,  # noqa: FBT001, ARG002
        include_value_samples: bool,  # noqa: FBT001, ARG002
    ) -> SemiStructuredProfileParseResult:
        """Mock profile_semi_structured_columns implementation."""
        if self.should_raise:
            raise self.should_raise
        if self.profile_result is not None:
            return self.profile_result

        return SemiStructuredProfileParseResult(
            total_rows=1000,
            sampled_rows=1000,
            column_profiles={},
            path_profiles=[],
            warnings=[],
        )
