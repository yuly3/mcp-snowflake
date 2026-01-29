from datetime import timedelta
from typing import Any


class MockExecuteQuery:
    """Mock implementation of EffectExecuteQuery protocol."""

    def __init__(
        self,
        result_data: list[dict[str, Any]] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise
        self.called_with_sql: str | None = None
        self.called_with_timeout: timedelta | None = None
        self.called_with_role: str | None = None
        self.called_with_warehouse: str | None = None

    async def execute_query(
        self,
        query: str,
        query_timeout: timedelta | None = None,
        role: str | None = None,
        warehouse: str | None = None,
    ) -> list[dict[str, Any]]:
        self.called_with_sql = query
        self.called_with_timeout = query_timeout
        self.called_with_role = role
        self.called_with_warehouse = warehouse
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            # Return minimal default data
            return [
                {"id": 1, "name": "test_result_1", "count": 100},
                {"id": 2, "name": "test_result_2", "count": 200},
            ]
        return self.result_data
