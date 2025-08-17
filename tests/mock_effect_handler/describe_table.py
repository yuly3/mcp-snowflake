from kernel.table_metadata import DataBase, Schema, TableInfo


class MockDescribeTable:
    """Mock implementation of EffectDescribeTable protocol."""

    def __init__(
        self,
        table_info: TableInfo | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.table_info = table_info
        self.should_raise = should_raise

    async def describe_table(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table: str,  # noqa: ARG002
    ) -> TableInfo:
        if self.should_raise:
            raise self.should_raise
        if self.table_info is None:
            # Return minimal default
            return TableInfo(
                database=DataBase("default_db"),
                schema=Schema("default_schema"),
                name="default_table",
                column_count=0,
                columns=[],
            )
        return self.table_info
