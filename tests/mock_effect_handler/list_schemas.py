from kernel.table_metadata import DataBase, Schema


class MockListSchemas:
    """Mock implementation of EffectListSchemas protocol."""

    def __init__(
        self,
        result_data: list[Schema] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise
        self.called_with_role: str | None = None
        self.called_with_warehouse: str | None = None

    async def list_schemas(
        self,
        database: DataBase,  # noqa: ARG002
        role: str | None = None,
        warehouse: str | None = None,
    ) -> list[Schema]:
        self.called_with_role = role
        self.called_with_warehouse = warehouse
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            # Return minimal default
            return [
                Schema("INFORMATION_SCHEMA"),
                Schema("PUBLIC"),
            ]
        return self.result_data
