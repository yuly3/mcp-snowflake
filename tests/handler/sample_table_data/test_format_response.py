from typing import Any

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.sample_table_data import _format_response


class TestFormatResponse:
    """Test _format_response function."""

    @pytest.mark.parametrize(
        ("case_id", "processed_rows", "warnings", "sample_size", "expected"),
        [
            (
                "no_warnings",
                [{"col1": "value1", "col2": 123}, {"col1": "value2", "col2": 456}],
                [],
                10,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col2"],
                    "warnings_len": 0,
                },
            ),
            (
                "with_warnings",
                [{"col1": "value1", "col2": "<unsupported_type: complex>"}],
                ["対応していない型の列 'col2' が含まれています"],
                5,
                {
                    "actual_rows": 1,
                    "columns": ["col1", "col2"],
                    "warnings_len": 1,
                },
            ),
            (
                "empty_data",
                [],
                [],
                10,
                {
                    "actual_rows": 0,
                    "columns": [],
                    "warnings_len": 0,
                },
            ),
        ],
        ids=["no_warnings", "with_warnings", "empty_data"],
    )
    def test_format_response_variants(
        self,
        case_id: str,
        processed_rows: list[dict[str, Any]],
        warnings: list[str],
        sample_size: int,
        expected: dict[str, Any],
    ) -> None:
        """Test _format_response function with various scenarios."""
        response = _format_response(
            processed_rows,
            warnings,
            DataBase("test_db"),
            Schema("test_schema"),
            Table("test_table"),
            sample_size,
        )

        sample_data = response["sample_data"]

        # Common assertions
        assert sample_data["database"] == "test_db", f"[{case_id}] Database mismatch"
        assert sample_data["schema"] == "test_schema", f"[{case_id}] Schema mismatch"
        assert sample_data["table"] == "test_table", f"[{case_id}] Table mismatch"
        assert sample_data["sample_size"] == sample_size, (
            f"[{case_id}] Sample size mismatch"
        )

        # Expected value assertions
        assert sample_data["actual_rows"] == expected["actual_rows"], (
            f"[{case_id}] Actual rows mismatch"
        )
        assert sample_data["columns"] == expected["columns"], (
            f"[{case_id}] Columns mismatch"
        )
        assert len(sample_data["warnings"]) == expected["warnings_len"], (
            f"[{case_id}] Warnings length mismatch"
        )
        assert sample_data["rows"] == processed_rows, f"[{case_id}] Rows mismatch"
        assert sample_data["warnings"] == warnings, (
            f"[{case_id}] Warnings content mismatch"
        )
