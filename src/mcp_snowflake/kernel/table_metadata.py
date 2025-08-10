"""Table metadata domain models using attrs."""

import attrs
from cattrs import Converter

# Default cattrs converter for this module
converter = Converter()


@attrs.define(frozen=True, slots=True)
class TableColumn:
    """Domain model for a table column."""

    name: str
    data_type: str
    nullable: bool
    ordinal_position: int
    default_value: str | None = None
    comment: str | None = None


@attrs.define(frozen=True, slots=True)
class TableInfo:
    """Domain model for table information."""

    database: str
    schema: str
    name: str
    column_count: int
    columns: list[TableColumn]
