import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, Table, TableColumn, TableInfo
from mcp_snowflake.handler import DescribeTableArgs, handle_describe_table

from ...mock_effect_handler import MockDescribeTable


class TestDescribeTableArgs:
    """Test DescribeTableArgs validation."""

    def test_describe_table_args_validation(self) -> None:
        """Test DescribeTableArgs validation."""
        args = DescribeTableArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
        )
        assert args.database == DataBase("test_db")
        assert args.schema_ == Schema("test_schema")
        assert args.table_ == Table("test_table")

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"schema": "test_schema", "table": "test_table"},
            )

    def test_missing_schema(self) -> None:
        """Test missing schema argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"database": "test_db", "table": "test_table"},
            )

    def test_missing_table(self) -> None:
        """Test missing table argument."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate(
                {"database": "test_db", "schema": "test_schema"},
            )

    def test_missing_all_args(self) -> None:
        """Test missing all arguments."""
        with pytest.raises(ValidationError):
            _ = DescribeTableArgs.model_validate({})

    def test_empty_strings(self) -> None:
        """Test empty string arguments."""
        args = DescribeTableArgs(database=DataBase(""), schema=Schema(""), table=Table(""))
        assert args.database == DataBase("")
        assert args.schema_ == Schema("")
        assert args.table_ == Table("")


class TestHandleDescribeTable:
    """Test handle_describe_table function."""

    @pytest.mark.parametrize(
        ("label", "database", "schema", "table", "columns_spec"),
        [
            (
                "base",
                "test_db",
                "test_schema",
                "test_table",
                [
                    TableColumn(
                        name="ID",
                        data_type="NUMBER(38,0)",
                        nullable=False,
                        default_value=None,
                        comment="Primary key",
                        ordinal_position=1,
                    ),
                    TableColumn(
                        name="NAME",
                        data_type="VARCHAR(100)",
                        nullable=True,
                        default_value=None,
                        comment="User name",
                        ordinal_position=2,
                    ),
                ],
            ),
            (
                "empty",
                "empty_db",
                "empty_schema",
                "empty_table",
                [],
            ),
        ],
        ids=["base", "empty"],
    )
    @pytest.mark.asyncio
    async def test_describe_table_returns_describe_table_result(
        self,
        label: str,
        database: str,
        schema: str,
        table: str,
        columns_spec: list[TableColumn],
    ) -> None:
        """Test handle_describe_table returns DescribeTableResult."""
        args = DescribeTableArgs(database=DataBase(database), schema=Schema(schema), table=Table(table))
        mock_table_data = TableInfo(
            database=DataBase(database),
            schema=Schema(schema),
            name=table,
            column_count=len(columns_spec),
            columns=columns_spec,
        )
        effect_handler = MockDescribeTable(table_info=mock_table_data)

        result = await handle_describe_table(args, effect_handler)

        assert result.database == database, f"[{label}] Database mismatch"
        assert result.schema == schema, f"[{label}] Schema mismatch"
        assert result.name == table, f"[{label}] Table name mismatch"
        assert result.column_count == len(columns_spec), f"[{label}] Column count mismatch"
        assert result.columns == columns_spec, f"[{label}] Columns mismatch"
