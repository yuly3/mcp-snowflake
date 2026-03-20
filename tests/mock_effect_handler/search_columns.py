from collections.abc import Sequence

from kernel.table_metadata import DataBase
from mcp_snowflake.handler.search_columns import SearchColumnsTableEntry


class MockSearchColumns:
    """Mock implementation of EffectSearchColumns protocol."""

    def __init__(
        self,
        result_data: Sequence[SearchColumnsTableEntry] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise

    async def search_columns(
        self,
        database: DataBase,  # noqa: ARG002
        column_name_pattern: str | None,  # noqa: ARG002
        data_type: str | None,  # noqa: ARG002
        schema: str | None,  # noqa: ARG002
        table_name_pattern: str | None,  # noqa: ARG002
        limit: int,  # noqa: ARG002
    ) -> Sequence[SearchColumnsTableEntry]:
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            return [
                SearchColumnsTableEntry(
                    schema="PUBLIC",
                    table="ORDERS",
                    columns_json='[{"name":"ORDER_ID","type":"NUMBER"}]',
                ),
                SearchColumnsTableEntry(
                    schema="PUBLIC",
                    table="CUSTOMERS",
                    columns_json='[{"name":"CUSTOMER_ID","type":"NUMBER"}]',
                ),
            ]
        return self.result_data
