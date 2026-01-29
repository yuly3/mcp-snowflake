from mcp_snowflake.handler.list_roles import RoleInfoDict


class MockListRoles:
    """Mock implementation of EffectListRoles protocol."""

    def __init__(
        self,
        result_data: list[RoleInfoDict] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise

    async def list_roles(self) -> list[RoleInfoDict]:
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            return [
                RoleInfoDict(name="PUBLIC", owner="ACCOUNTADMIN", comment=None),
                RoleInfoDict(name="ACCOUNTADMIN", owner=None, comment="Account admin"),
            ]
        return self.result_data
