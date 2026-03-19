import pytest

from kernel.table_metadata import DataBase
from mcp_snowflake.handler import handle_list_databases

from ...mock_effect_handler import MockListDatabases


class TestHandleListDatabases:
    """Test handle_list_databases function."""

    @pytest.mark.asyncio
    async def test_successful_list_databases(self) -> None:
        """Test successful database listing returns ListDatabasesResult."""
        mock_databases = [DataBase("DB1"), DataBase("DB2"), DataBase("DB3")]
        effect_handler = MockListDatabases(result_data=mock_databases)

        result = await handle_list_databases(effect_handler)

        assert result.databases == ["DB1", "DB2", "DB3"]

    @pytest.mark.asyncio
    async def test_empty_databases_list(self) -> None:
        """Test when no databases are returned."""
        effect_handler = MockListDatabases(result_data=[])

        result = await handle_list_databases(effect_handler)

        assert result.databases == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        error_message = "Connection failed"
        effect_handler = MockListDatabases(should_raise=Exception(error_message))

        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_databases(effect_handler)

    @pytest.mark.asyncio
    async def test_single_database(self) -> None:
        """Test with single database result."""
        effect_handler = MockListDatabases(result_data=[DataBase("ONLY_DB")])

        result = await handle_list_databases(effect_handler)

        assert result.databases == ["ONLY_DB"]
