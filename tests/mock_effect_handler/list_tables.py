from kernel.table_metadata import DataBase, Schema, Table


class MockListTables:
    """Mock implementation of EffectListTables protocol."""

    def __init__(
        self,
        result_data: list[Table] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise

    async def list_tables(
        self,
        database: DataBase,  # noqa: ARG002
        schema: Schema,  # noqa: ARG002
    ) -> list[Table]:
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            # Return minimal default
            return [
                Table("CUSTOMERS"),
                Table("ORDERS"),
            ]
        return self.result_data
