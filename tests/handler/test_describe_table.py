import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, Table, TableColumn, TableInfo
from mcp_snowflake.handler import DescribeTableArgs, handle_describe_table

from ..mock_effect_handler import MockDescribeTable


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
        ("label", "database", "schema", "table", "columns_spec", "expectations"),
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
                {"has_nullable": True, "has_required": True},
            ),
            (
                "empty",
                "empty_db",
                "empty_schema",
                "empty_table",
                [],
                {"empty": True},
            ),
            (
                "all_nullable",
                "nullable_db",
                "nullable_schema",
                "nullable_table",
                [
                    TableColumn(
                        name="OPTIONAL1",
                        data_type="VARCHAR(50)",
                        nullable=True,
                        default_value=None,
                        comment="Optional field 1",
                        ordinal_position=1,
                    ),
                    TableColumn(
                        name="OPTIONAL2",
                        data_type="INTEGER",
                        nullable=True,
                        default_value=None,
                        comment="Optional field 2",
                        ordinal_position=2,
                    ),
                ],
                {"all_nullable": True},
            ),
            (
                "all_required",
                "required_db",
                "required_schema",
                "required_table",
                [
                    TableColumn(
                        name="REQUIRED1",
                        data_type="VARCHAR(50)",
                        nullable=False,
                        default_value=None,
                        comment="Required field 1",
                        ordinal_position=1,
                    ),
                    TableColumn(
                        name="REQUIRED2",
                        data_type="INTEGER",
                        nullable=False,
                        default_value=None,
                        comment="Required field 2",
                        ordinal_position=2,
                    ),
                ],
                {"all_required": True},
            ),
        ],
        ids=["base", "empty", "all_nullable", "all_required"],
    )
    @pytest.mark.asyncio
    async def test_describe_table_success_variants(
        self,
        label: str,
        database: str,
        schema: str,
        table: str,
        columns_spec: list[TableColumn],
        expectations: dict[str, bool],
    ) -> None:
        """Test successful table description scenarios with parametrized variants."""
        # Arrange
        args = DescribeTableArgs(database=DataBase(database), schema=Schema(schema), table=Table(table))
        mock_table_data = TableInfo(
            database=DataBase(database),
            schema=Schema(schema),
            name=table,
            column_count=len(columns_spec),
            columns=columns_spec,
        )
        effect_handler = MockDescribeTable(table_info=mock_table_data)

        # Act
        result = await handle_describe_table(args, effect_handler)

        table_info = result["table_info"]

        # Basic table info validation
        assert table_info["database"] == database, f"[{label}] Database mismatch"
        assert table_info["schema"] == schema, f"[{label}] Schema mismatch"
        assert table_info["name"] == table, f"[{label}] Table name mismatch"
        assert table_info["column_count"] == len(columns_spec), f"[{label}] Column count mismatch"

        # Scenario-specific validations
        if expectations.get("empty"):
            assert table_info["columns"] == [], f"[{label}] Expected empty columns"
        else:
            assert len(table_info["columns"]) == len(columns_spec), f"[{label}] Column list length mismatch"

        if expectations.get("all_nullable"):
            for column in table_info["columns"]:
                assert column["nullable"] is True, f"[{label}] Column {column['name']} should be nullable"

        if expectations.get("all_required"):
            for column in table_info["columns"]:
                assert column["nullable"] is False, f"[{label}] Column {column['name']} should be required"

        if expectations.get("has_nullable") and expectations.get("has_required"):
            nullable_cols = [col for col in table_info["columns"] if col["nullable"]]
            required_cols = [col for col in table_info["columns"] if not col["nullable"]]
            assert len(nullable_cols) > 0, f"[{label}] Expected at least one nullable column"
            assert len(required_cols) > 0, f"[{label}] Expected at least one required column"
