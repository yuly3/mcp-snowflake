"""Describe table result model and serializers.

This module provides a format-agnostic DescribeTableResult class and a serializer
interface following the double-dispatch (Visitor) pattern, inspired by
Rust's serde.  The data structure drives the serialization by calling
back into the serializer, while each serializer controls the output format.
"""

from abc import ABC, abstractmethod

import attrs

from kernel.table_metadata import DataBase, Schema, TableColumn


@attrs.define(frozen=True)
class DescribeTableResult:
    """Format-agnostic representation of a describe table result.

    This class holds the raw result data and delegates formatting to an
    external ``DescribeTableResultSerializer`` via the :meth:`serialize_with`
    method (double dispatch).

    Attributes
    ----------
    database : DataBase
        Database name.
    schema : Schema
        Schema name.
    name : str
        Table name.
    column_count : int
        Number of columns in the table.
    columns : list[TableColumn]
        Ordered list of table columns.
    """

    database: DataBase
    schema: Schema
    name: str
    column_count: int
    columns: list[TableColumn]

    def serialize_with(self, serializer: "DescribeTableResultSerializer") -> str:
        """Serialize this result using the given serializer.

        The data structure drives the serialization: it calls back into
        the *serializer* for each logical piece of the result (metadata,
        columns).  This is the "accept" side of the Visitor /
        double-dispatch pattern.

        Parameters
        ----------
        serializer : DescribeTableResultSerializer
            The serializer (visitor) to use.

        Returns
        -------
        str
            The serialized string representation.
        """
        serializer.visit_metadata(self.database, self.schema, self.name, self.column_count)
        for index, column in enumerate(self.columns):
            serializer.visit_column(index, column)
        return serializer.finish()


class DescribeTableResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`DescribeTableResult`.

    Concrete subclasses implement each ``visit_*`` / ``finish`` method to
    produce a specific output format (JSON, compact text, etc.).
    """

    @abstractmethod
    def visit_metadata(
        self,
        database: DataBase,
        schema: Schema,
        name: str,
        column_count: int,
    ) -> None:
        """Receive table-level metadata."""
        ...

    @abstractmethod
    def visit_column(self, index: int, column: TableColumn) -> None:
        """Receive a single column description.

        Parameters
        ----------
        index : int
            Zero-based column index.
        column : TableColumn
            Column metadata.
        """
        ...

    @abstractmethod
    def finish(self) -> str:
        """Produce the final serialized string."""
        ...


# ---------------------------------------------------------------------------
# Concrete serializers
# ---------------------------------------------------------------------------


class CompactDescribeTableResultSerializer(DescribeTableResultSerializer):
    """Serialize a :class:`DescribeTableResult` in a compact, token-efficient format.

    Output example::

        database: test_db
        schema: test_schema
        table: test_table
        column_count: 2

        col1:
        name: ID
        type: NUMBER(38,0)
        nullable: false
        comment: Primary key

        col2:
        name: NAME
        type: VARCHAR(100)
        nullable: true

    Optional fields (``default_value``, ``comment``) are omitted when ``None``
    to minimise token usage.
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(
        self,
        database: DataBase,
        schema: Schema,
        name: str,
        column_count: int,
    ) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema: {schema}")
        self._lines.append(f"table: {name}")
        self._lines.append(f"column_count: {column_count}")

    def visit_column(self, index: int, column: TableColumn) -> None:
        self._lines.append("")
        self._lines.append(f"col{index + 1}:")
        self._lines.append(f"name: {column.name}")
        self._lines.append(f"type: {column.data_type.raw_type}")
        self._lines.append(f"nullable: {str(column.nullable).lower()}")
        if column.default_value is not None:
            self._lines.append(f"default: {column.default_value}")
        if column.comment:
            self._lines.append(f"comment: {column.comment}")

    def finish(self) -> str:
        return "\n".join(self._lines)
