import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase
from mcp_snowflake.handler import SearchColumnsArgs, handle_search_columns
from mcp_snowflake.handler.search_columns import SearchColumnsTableEntry

from ...mock_effect_handler import MockSearchColumns


class TestSearchColumnsArgs:
    """Test SearchColumnsArgs validation."""

    def test_valid_args_with_column_name_pattern(self) -> None:
        args = SearchColumnsArgs(database=DataBase("db"), column_name_pattern="%id%")
        assert args.column_name_pattern == "%id%"

    def test_valid_args_with_data_type(self) -> None:
        args = SearchColumnsArgs(database=DataBase("db"), data_type="VARIANT")
        assert args.data_type == "VARIANT"

    def test_valid_args_with_both_filters(self) -> None:
        args = SearchColumnsArgs(database=DataBase("db"), column_name_pattern="%id%", data_type="NUMBER")
        assert args.column_name_pattern == "%id%"
        assert args.data_type == "NUMBER"

    def test_missing_both_filters_raises(self) -> None:
        with pytest.raises(ValidationError, match="At least one"):
            _ = SearchColumnsArgs(database=DataBase("db"))

    def test_missing_database_raises(self) -> None:
        with pytest.raises(ValidationError):
            _ = SearchColumnsArgs.model_validate({"column_name_pattern": "%id%"})

    def test_optional_schema_filter(self) -> None:
        args = SearchColumnsArgs.model_validate({
            "database": "db",
            "column_name_pattern": "%id%",
            "schema": "PUBLIC",
        })
        assert args.schema_ == "PUBLIC"

    def test_optional_table_name_pattern(self) -> None:
        args = SearchColumnsArgs.model_validate({
            "database": "db",
            "data_type": "NUMBER",
            "table_name_pattern": "%ORDER%",
        })
        assert args.table_name_pattern == "%ORDER%"

    def test_limit_default(self) -> None:
        args = SearchColumnsArgs(database=DataBase("db"), data_type="NUMBER")
        assert args.limit == 50

    def test_limit_out_of_range(self) -> None:
        with pytest.raises(ValidationError):
            _ = SearchColumnsArgs(database=DataBase("db"), data_type="NUMBER", limit=201)


class TestHandleSearchColumns:
    """Test handle_search_columns function."""

    @pytest.mark.asyncio
    async def test_successful_search(self) -> None:
        args = SearchColumnsArgs(database=DataBase("db"), column_name_pattern="%id%")
        effect = MockSearchColumns()

        result = await handle_search_columns(args, effect)

        assert result.database == "db"
        assert result.table_count == 2

    @pytest.mark.asyncio
    async def test_empty_results(self) -> None:
        args = SearchColumnsArgs(database=DataBase("db"), data_type="VARIANT")
        effect = MockSearchColumns(result_data=[])

        result = await handle_search_columns(args, effect)

        assert result.table_count == 0
        assert result.tables == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        args = SearchColumnsArgs(database=DataBase("db"), data_type="NUMBER")
        effect = MockSearchColumns(should_raise=Exception("Connection failed"))

        with pytest.raises(Exception, match="Connection failed"):
            _ = await handle_search_columns(args, effect)

    @pytest.mark.asyncio
    async def test_custom_result(self) -> None:
        entries = [
            SearchColumnsTableEntry(
                schema="SCH",
                table="TBL",
                columns_json='[{"name":"COL","type":"TEXT","comment":"test"}]',
            ),
        ]
        args = SearchColumnsArgs(database=DataBase("db"), column_name_pattern="%col%")
        effect = MockSearchColumns(result_data=entries)

        result = await handle_search_columns(args, effect)

        assert result.table_count == 1
        assert result.tables[0].schema == "SCH"
        assert result.tables[0].table == "TBL"
