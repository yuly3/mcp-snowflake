"""SQL analysis functionality module.

Provides functionality to analyze SQL queries and detect Write operations.
Also supports Snowflake-specific syntax.

Classes
-------
SQLWriteDetector
    SQL Write operation detector
SQLAnalysisError
    SQL analysis error

TypedDict Classes
-----------------
StatementInfo
    SQL statement information
SQLAnalysisResult
    SQL analysis result
"""

from typing import ClassVar, NotRequired, TypedDict

import sqlparse
import sqlparse.sql


class StatementInfo(TypedDict):
    """SQL statement information.

    Attributes
    ----------
    index : int
        Statement index
    type : str
        Statement type (SELECT, INSERT, etc.)
    first_token : str | None
        First token value
    is_write : bool
        Whether it's a write operation
    """

    index: int
    type: str
    first_token: str | None
    is_write: bool


class SQLAnalysisResult(TypedDict):
    """SQL analysis result.

    Attributes
    ----------
    is_write : bool
        Whether it contains write operations
    statements : list[StatementInfo]
        Information for each statement
    error : str
        Error message (only when error occurs)
    """

    is_write: bool
    statements: list[StatementInfo]
    error: NotRequired[str]


class SQLAnalysisError(Exception):
    """SQL analysis error."""


class SQLWriteDetector:
    """SQL Write operation detector.

    Class that analyzes SQL queries to detect Write operations.
    Also supports Snowflake-specific syntax.

    Attributes
    ----------
    WRITE_KEYWORDS : ClassVar[set[str]]
        Keywords to be determined as Write operations
    READ_KEYWORDS : ClassVar[set[str]]
        Keywords to be determined as Read operations
    """

    # Keywords to be determined as Write operations
    WRITE_KEYWORDS: ClassVar[set[str]] = {
        # DML Write operations
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "TRUNCATE",
        # DDL operations
        "CREATE",
        "DROP",
        "ALTER",
        # Snowflake-specific Write operations
        "COPY",  # COPY INTO is usually a Write operation
        # Other Write operations
        "GRANT",
        "REVOKE",  # Permission operations
    }

    # Keywords to be determined as Read operations
    READ_KEYWORDS: ClassVar[set[str]] = {
        "SELECT",
        "WITH",  # Query operations
        "SHOW",
        "DESCRIBE",
        "DESC",  # Metadata operations
        "EXPLAIN",  # Execution plan
    }

    def is_write_sql(self, sql: str) -> bool:
        """Determine whether SQL is a Write operation.

        Parameters
        ----------
        sql : str
            SQL statement to be determined

        Returns
        -------
        bool
            True if it's a Write operation, False if it's a Read operation

        Raises
        ------
        SQLAnalysisError
            When SQL analysis fails
        """
        if not sql or not sql.strip():
            raise SQLAnalysisError("Empty SQL statement")

        try:
            # Parse SQL
            parsed_statements = sqlparse.parse(sql)
        except Exception as e:
            raise SQLAnalysisError(f"SQL parsing error: {e!s}") from e

        if not parsed_statements:
            raise SQLAnalysisError("Failed to parse SQL")

        # If there are multiple statements, determine as Write if at least one is a Write operation
        for statement in parsed_statements:
            if self._is_write_statement(statement):
                return True

        return False

    def _is_write_statement(self, statement: sqlparse.sql.Statement) -> bool:
        """Determine whether an individual statement is a Write operation.

        Parameters
        ----------
        statement : sqlparse.sql.Statement
            Parsed SQL statement

        Returns
        -------
        bool
            True if it's a Write operation
        """
        # Determination by statement type
        stmt_type = statement.get_type()

        if stmt_type in {
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TRUNCATE",
            "MERGE",
        }:
            return True

        # For UNKNOWN type, determine by the first token
        if stmt_type == "UNKNOWN":
            first_token = statement.token_first()
            if first_token:
                first_keyword = first_token.value.upper()

                # Check Write operation keywords
                if first_keyword in self.WRITE_KEYWORDS:
                    return True

                # Check Read operation keywords
                # Unknown keywords are considered Write for safety
                return first_keyword not in self.READ_KEYWORDS

        # SELECT is clearly a Read operation, other unknown types are considered Write for safety
        return stmt_type not in ("SELECT",)

    def analyze_sql(self, sql: str) -> SQLAnalysisResult:
        """Return detailed analysis result of SQL.

        Parameters
        ----------
        sql : str
            SQL statement to be analyzed

        Returns
        -------
        SQLAnalysisResult
            Detailed information of the analysis result
        """
        result: SQLAnalysisResult = {
            "is_write": False,
            "statements": [],
        }

        try:
            result["is_write"] = self.is_write_sql(sql)

            # Detailed information for each statement
            parsed_statements = sqlparse.parse(sql)
            for i, statement in enumerate(parsed_statements):
                first_token = statement.token_first()
                stmt_info: StatementInfo = {
                    "index": i,
                    "type": statement.get_type(),
                    "first_token": first_token.value if first_token else None,
                    "is_write": self._is_write_statement(statement),
                }
                result["statements"].append(stmt_info)

        except SQLAnalysisError as e:
            result["error"] = str(e)
            result["is_write"] = True  # Consider as Write for safety in case of error

        return result
