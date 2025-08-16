from typing import Any

import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.sample_table_data import SampleTableDataArgs


class TestSampleTableDataArgs:
    """Test SampleTableDataArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = SampleTableDataArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
        )
        assert args.database == "test_db"
        assert args.schema_ == "test_schema"
        assert args.table_ == "test_table"
        assert args.sample_size == 10  # default
        assert args.columns == []  # default

    def test_valid_args_all_fields(self) -> None:
        """Test valid arguments with all fields specified."""
        args = SampleTableDataArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            sample_size=20,
            columns=["col1", "col2"],
        )
        assert args.database == "test_db"
        assert args.schema_ == "test_schema"
        assert args.table_ == "test_table"

    @pytest.mark.parametrize(
        ("case_id", "payload"),
        [
            ("missing_database", {"schema": "test_schema", "table": "test_table"}),
            ("missing_schema", {"database": "test_db", "table": "test_table"}),
            ("missing_table", {"database": "test_db", "schema": "test_schema"}),
            (
                "invalid_sample_size_type",
                {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table",
                    "sample_size": "invalid",
                },
            ),
        ],
        ids=[
            "missing_database",
            "missing_schema",
            "missing_table",
            "invalid_sample_size_type",
        ],
    )
    def test_args_validation_errors(
        self,
        case_id: str,  # noqa: ARG002
        payload: dict[str, Any],
    ) -> None:
        """Test argument validation error scenarios."""
        with pytest.raises(ValidationError, match=r".*"):
            _ = SampleTableDataArgs.model_validate(payload)
