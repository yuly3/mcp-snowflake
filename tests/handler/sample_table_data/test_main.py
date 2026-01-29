from typing import Any

import pytest

from cattrs_converter import JsonImmutableConverter
from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.sample_table_data import (
    SampleTableDataArgs,
    handle_sample_table_data,
)


class MockEffectSampleTableData:
    """Mock implementation of EffectSampleTableData protocol."""

    def __init__(
        self,
        sample_data: list[dict[str, Any]] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.sample_data = sample_data or []
        self.should_raise = should_raise

    async def sample_table_data(
        self,
        database: DataBase,  # noqa: ARG002
        schema: Schema,  # noqa: ARG002
        table: Table,  # noqa: ARG002
        sample_size: int,  # noqa: ARG002
        columns: list[str],  # noqa: ARG002
        role: str | None = None,  # noqa: ARG002
        warehouse: str | None = None,  # noqa: ARG002
    ) -> list[dict[str, Any]]:
        if self.should_raise:
            raise self.should_raise
        return self.sample_data


class TestHandleSampleTableData:
    """Test handle_sample_table_data function."""

    @pytest.mark.parametrize(
        ("case_id", "sample_data", "columns_arg", "sample_size", "expected"),
        [
            (
                "basic",
                [{"col1": "value1", "col2": 123}, {"col1": "value2", "col2": 456}],
                None,
                10,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col2"],
                    "warnings_len": 0,
                },
            ),
            (
                "non_serializable",
                [
                    {"col1": "value1", "col2": 1 + 2j},
                    {"col1": "value2", "col2": 3 + 4j},
                ],
                None,
                10,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col2"],
                    "warnings_contains": "Column 'col2' contains unsupported data type",
                    "mutated_field": ("col2", "<unsupported_type: complex>"),
                },
            ),
            (
                "empty",
                [],
                None,
                10,
                {
                    "actual_rows": 0,
                    "columns": [],
                    "warnings_len": 0,
                },
            ),
            (
                "with_columns",
                [{"col1": "v1", "col3": "a"}, {"col1": "v2", "col3": "b"}],
                ["col1", "col3"],
                5,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col3"],
                    "warnings_len": 0,
                },
            ),
        ],
        ids=["basic", "non_serializable", "empty", "with_columns"],
    )
    @pytest.mark.asyncio
    async def test_handle_sample_table_data_success_variants(
        self,
        json_converter: JsonImmutableConverter,
        case_id: str,
        sample_data: list[dict[str, Any]],
        columns_arg: list[str] | None,
        sample_size: int,
        expected: dict[str, Any],
    ) -> None:
        """Test successful sample data retrieval scenarios with parametrized variants."""
        # Arrange
        mock_effect = MockEffectSampleTableData(sample_data=sample_data)
        args = SampleTableDataArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            sample_size=sample_size,
            columns=columns_arg or [],
        )

        # Act
        result = await handle_sample_table_data(json_converter, args, mock_effect)

        # Assert - result should be SampleTableDataJsonResponse directly
        assert isinstance(result, dict)
        assert "sample_data" in result
        sample_data_obj = result["sample_data"]

        # Common assertions
        assert sample_data_obj["database"] == "test_db", (
            f"[{case_id}] Database mismatch"
        )
        assert sample_data_obj["schema"] == "test_schema", (
            f"[{case_id}] Schema mismatch"
        )
        assert sample_data_obj["table"] == "test_table", f"[{case_id}] Table mismatch"
        assert sample_data_obj["sample_size"] == sample_size, (
            f"[{case_id}] Sample size mismatch"
        )

        # Expected value assertions
        assert sample_data_obj["actual_rows"] == expected["actual_rows"], (
            f"[{case_id}] Actual rows mismatch"
        )
        assert sample_data_obj["columns"] == expected["columns"], (
            f"[{case_id}] Columns mismatch"
        )

        # Conditional assertions based on expected keys
        if "warnings_len" in expected:
            assert len(sample_data_obj["warnings"]) == expected["warnings_len"], (
                f"[{case_id}] Warnings length mismatch"
            )

        if "warnings_contains" in expected:
            assert any(
                expected["warnings_contains"] in warning
                for warning in sample_data_obj["warnings"]
            ), f"[{case_id}] Warning content not found"

        if "mutated_field" in expected:
            column_name, expected_value = expected["mutated_field"]
            actual_value = sample_data_obj["rows"][0][column_name]
            assert actual_value == expected_value, (
                f"[{case_id}] Mutated field {column_name} mismatch"
            )

        # For non-empty cases, verify rows structure
        if expected["actual_rows"] > 0 and "mutated_field" not in expected:
            assert sample_data_obj["rows"] == sample_data, (
                f"[{case_id}] Rows content mismatch"
            )

    @pytest.mark.asyncio
    async def test_error_handling(self, json_converter: JsonImmutableConverter) -> None:
        """Test error handling when effect handler raises exception."""
        mock_effect = MockEffectSampleTableData(
            should_raise=Exception("Database error"),
        )

        args = SampleTableDataArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
        )

        # Since we refactored the handler to not handle exceptions,
        # the exception should be raised directly
        with pytest.raises(Exception, match="Database error"):
            _ = await handle_sample_table_data(json_converter, args, mock_effect)
