from typing import Any

from kernel.table_metadata import DataBase, Schema, Table


class MockSampleTableData:
    """Mock implementation of EffectSampleTableData protocol."""

    def __init__(
        self,
        result_data: list[dict[str, Any]] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise
        self.called_with_role: str | None = None
        self.called_with_warehouse: str | None = None

    async def sample_table_data(
        self,
        database: DataBase,  # noqa: ARG002
        schema: Schema,  # noqa: ARG002
        table: Table,  # noqa: ARG002
        sample_size: int,  # noqa: ARG002
        columns: list[str],  # noqa: ARG002
        role: str | None = None,
        warehouse: str | None = None,
    ) -> list[dict[str, Any]]:
        self.called_with_role = role
        self.called_with_warehouse = warehouse
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            # Return minimal default data
            return [
                {"id": 1, "name": "test_row_1", "value": 10.5},
                {"id": 2, "name": "test_row_2", "value": 20.0},
            ]
        return self.result_data
