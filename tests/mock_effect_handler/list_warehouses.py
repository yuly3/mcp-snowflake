from mcp_snowflake.handler.list_warehouses import WarehouseInfoDict


class MockListWarehouses:
    """Mock implementation of EffectListWarehouses protocol."""

    def __init__(
        self,
        result_data: list[WarehouseInfoDict] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise
        self.last_role: str | None = None

    async def list_warehouses(
        self,
        role: str | None = None,
    ) -> list[WarehouseInfoDict]:
        self.last_role = role
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            return [
                WarehouseInfoDict(
                    name="COMPUTE_WH",
                    state="STARTED",
                    size="X-Small",
                    owner="ACCOUNTADMIN",
                    comment=None,
                ),
                WarehouseInfoDict(
                    name="DEV_WH",
                    state="SUSPENDED",
                    size="Small",
                    owner="SYSADMIN",
                    comment="Development warehouse",
                ),
            ]
        return self.result_data
