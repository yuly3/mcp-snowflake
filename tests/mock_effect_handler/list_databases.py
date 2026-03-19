from kernel.table_metadata import DataBase


class MockListDatabases:
    """Mock implementation of EffectListDatabases protocol."""

    def __init__(
        self,
        result_data: list[DataBase] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise

    async def list_databases(self) -> list[DataBase]:
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            return [
                DataBase("ANALYTICS"),
                DataBase("RAW"),
            ]
        return self.result_data
