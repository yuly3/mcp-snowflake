"""Internal classes for semi-structured column profiling."""

import attrs

from kernel.table_metadata import TableColumn


@attrs.define(frozen=True, slots=True)
class ClassifiedSemiStructuredColumns:
    """Result of semi-structured column classification."""

    supported_columns: list[TableColumn]
    unsupported_columns: list[TableColumn]
