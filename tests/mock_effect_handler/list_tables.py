from kernel.table_metadata import DataBase, ObjectKind, Schema, SchemaObject


class MockListTables:
    """Mock implementation of EffectListTables protocol."""

    def __init__(
        self,
        result_data: list[SchemaObject] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise

    async def list_objects(
        self,
        database: DataBase,  # noqa: ARG002
        schema: Schema,  # noqa: ARG002
    ) -> list[SchemaObject]:
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            # Return minimal default
            return [
                SchemaObject(name="CUSTOMERS", kind=ObjectKind.TABLE),
                SchemaObject(name="ORDERS", kind=ObjectKind.TABLE),
                SchemaObject(name="CUSTOMER_VIEW", kind=ObjectKind.VIEW),
            ]
        return self.result_data
