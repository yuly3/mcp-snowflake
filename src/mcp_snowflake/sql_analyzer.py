"""SQL解析機能モジュール.

SQLクエリを解析してWrite操作を検出する機能を提供します。
Snowflake専用構文にも対応しています。

Classes
-------
SQLWriteDetector
    SQL Write操作検知器
SQLAnalysisError
    SQL解析エラー

TypedDict Classes
-----------------
StatementInfo
    SQLステートメントの情報
SQLAnalysisResult
    SQL解析結果
"""

from typing import ClassVar, NotRequired, TypedDict

import sqlparse
import sqlparse.sql


class StatementInfo(TypedDict):
    """SQLステートメントの情報.

    Attributes
    ----------
    index : int
        ステートメントのインデックス
    type : str
        ステートメントのタイプ（SELECT, INSERT等）
    first_token : str | None
        最初のトークンの値
    is_write : bool
        Write操作かどうか
    """

    index: int
    type: str
    first_token: str | None
    is_write: bool


class SQLAnalysisResult(TypedDict):
    """SQL解析結果.

    Attributes
    ----------
    is_write : bool
        Write操作を含むかどうか
    statements : list[StatementInfo]
        各ステートメントの情報
    error : str | None, optional
        エラーメッセージ（エラー時のみ）
    """

    is_write: bool
    statements: list[StatementInfo]
    error: NotRequired[str | None]


class SQLAnalysisError(Exception):
    """SQL解析エラー."""


class SQLWriteDetector:
    """SQL Write操作検知器.

    SQLクエリを解析してWrite操作を検出するクラス。
    Snowflake専用構文にも対応しています。

    Attributes
    ----------
    WRITE_KEYWORDS : ClassVar[set[str]]
        Write操作と判定するキーワード
    READ_KEYWORDS : ClassVar[set[str]]
        Read操作と判定するキーワード
    """

    # Write操作と判定するキーワード
    WRITE_KEYWORDS: ClassVar[set[str]] = {
        # DML Write操作
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "TRUNCATE",
        # DDL操作
        "CREATE",
        "DROP",
        "ALTER",
        # Snowflake専用のWrite操作
        "COPY",  # COPY INTOは通常Write操作
        # その他のWrite操作
        "GRANT",
        "REVOKE",  # 権限操作
    }

    # Read操作と判定するキーワード
    READ_KEYWORDS: ClassVar[set[str]] = {
        "SELECT",
        "WITH",  # クエリ操作
        "SHOW",
        "DESCRIBE",
        "DESC",  # メタデータ操作
        "EXPLAIN",  # 実行計画
    }

    def is_write_sql(self, sql: str) -> bool:
        """SQLがWrite操作かどうかを判定する.

        Parameters
        ----------
        sql : str
            判定対象のSQL文

        Returns
        -------
        bool
            Write操作の場合True、Read操作の場合False

        Raises
        ------
        SQLAnalysisError
            SQL解析に失敗した場合
        """
        if not sql or not sql.strip():
            raise SQLAnalysisError("Empty SQL statement")

        try:
            # SQLを解析
            parsed_statements = sqlparse.parse(sql)
        except Exception as e:
            raise SQLAnalysisError(f"SQL parsing error: {e!s}") from e

        if not parsed_statements:
            raise SQLAnalysisError("Failed to parse SQL")

        # 複数のステートメントがある場合、1つでもWrite操作があればWriteと判定
        for statement in parsed_statements:
            if self._is_write_statement(statement):
                return True

        return False

    def _is_write_statement(self, statement: sqlparse.sql.Statement) -> bool:
        """個別のステートメントがWrite操作かどうかを判定する.

        Parameters
        ----------
        statement : sqlparse.sql.Statement
            解析されたSQLステートメント

        Returns
        -------
        bool
            Write操作の場合True
        """
        # ステートメントタイプによる判定
        stmt_type = statement.get_type()

        if stmt_type in (
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TRUNCATE",
            "MERGE",
        ):
            return True

        # UNKNOWNの場合は最初のトークンで判定
        if stmt_type == "UNKNOWN":
            first_token = statement.token_first()
            if first_token:
                first_keyword = first_token.value.upper()

                # Write操作キーワードの確認
                if first_keyword in self.WRITE_KEYWORDS:
                    return True

                # Read操作キーワードの確認
                # 不明なキーワードは安全のためWriteと判定
                return first_keyword not in self.READ_KEYWORDS

        # SELECTは明確にRead操作、その他の不明なタイプは安全のためWriteと判定
        return stmt_type not in ("SELECT",)

    def analyze_sql(self, sql: str) -> SQLAnalysisResult:
        """SQLの詳細な解析結果を返す.

        Parameters
        ----------
        sql : str
            解析対象のSQL文

        Returns
        -------
        SQLAnalysisResult
            解析結果の詳細情報
        """
        result: SQLAnalysisResult = {
            "is_write": False,
            "statements": [],
        }

        try:
            result["is_write"] = self.is_write_sql(sql)

            # 各ステートメントの詳細情報
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
            result["is_write"] = True  # エラーの場合は安全のためWriteと判定

        return result
