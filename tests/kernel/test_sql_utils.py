"""Tests for SQL utilities (identifier quoting)."""

import pytest

from mcp_snowflake.kernel.sql_utils import fully_qualified, quote_ident


class TestQuoteIdent:
    """Test cases for quote_ident function."""

    def test_quote_ident_simple_pass_through(self) -> None:
        """Simple uppercase identifiers should pass through unchanged."""
        assert quote_ident("MYTABLE") == "MYTABLE"
        assert quote_ident("USER_TABLE") == "USER_TABLE"
        assert quote_ident("A") == "A"
        assert quote_ident("_PRIVATE") == "_PRIVATE"

    def test_quote_ident_needs_quoting(self) -> None:
        """Identifiers that don't match simple pattern should be quoted."""
        assert quote_ident("MyTable") == '"MyTable"'
        assert quote_ident("my_table") == '"my_table"'
        assert quote_ident("user-table") == '"user-table"'
        assert quote_ident("table name") == '"table name"'
        assert quote_ident("123table") == '"123table"'

    def test_quote_ident_escapes_internal_quotes(self) -> None:
        """Double quotes inside identifiers should be escaped."""
        assert quote_ident('table"name') == '"table""name"'
        assert quote_ident('A"B"C') == '"A""B""C"'
        assert quote_ident('"already"') == '"""already"""'

    def test_quote_ident_invalid_empty(self) -> None:
        """Empty or whitespace-only identifiers should raise ValueError."""
        with pytest.raises(ValueError, match="Empty identifier"):
            _ = quote_ident("")

        with pytest.raises(ValueError, match="Empty identifier"):
            _ = quote_ident("   ")

    def test_quote_ident_trims_whitespace(self) -> None:
        """Leading/trailing whitespace should be trimmed."""
        assert quote_ident("  MYTABLE  ") == "MYTABLE"
        assert quote_ident(" my table ") == '"my table"'


class TestFullyQualified:
    """Test cases for fully_qualified function."""

    def test_fully_qualified_with_schema(self) -> None:
        """Three-part identifier should quote each part as needed."""
        assert fully_qualified("MYDB", "MYSCHEMA", "MYTABLE") == "MYDB.MYSCHEMA.MYTABLE"
        assert (
            fully_qualified("my_db", "my_schema", "my_table")
            == '"my_db"."my_schema"."my_table"'
        )
        assert fully_qualified("DB", "schema name", "TABLE") == 'DB."schema name".TABLE'

    def test_fully_qualified_without_schema(self) -> None:
        """Two-part identifier when schema is None."""
        assert fully_qualified("MYDB", None, "MYTABLE") == "MYDB.MYTABLE"
        assert fully_qualified("my_db", None, "my_table") == '"my_db"."my_table"'

    def test_fully_qualified_escaping(self) -> None:
        """Identifiers with quotes should be properly escaped."""
        assert (
            fully_qualified('db"name', "schema", 'table"name')
            == '"db""name"."schema"."table""name"'
        )
