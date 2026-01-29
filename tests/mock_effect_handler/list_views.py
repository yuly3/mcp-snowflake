from kernel.table_metadata import DataBase, Schema, View


class MockListViews:
    """Mock implementation of EffectListViews protocol."""

    def __init__(
        self,
        result_data: list[View] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise
        self.called_with_role: str | None = None
        self.called_with_warehouse: str | None = None

    async def list_views(
        self,
        database: DataBase,  # noqa: ARG002
        schema: Schema,  # noqa: ARG002
        role: str | None = None,
        warehouse: str | None = None,
    ) -> list[View]:
        self.called_with_role = role
        self.called_with_warehouse = warehouse
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            # Return minimal default
            return [
                View("CUSTOMER_VIEW"),
                View("ORDER_SUMMARY_VIEW"),
            ]
        return self.result_data
