"""Column analysis utilities for semi-structured profiling."""

from collections.abc import Sequence

from kernel.table_metadata import TableColumn

from ._types import ClassifiedSemiStructuredColumns
from .models import NoSemiStructuredColumns, SemiStructuredColumnDoesNotExist

SEMI_STRUCTURED_TYPES = frozenset({"VARIANT", "ARRAY", "OBJECT"})


def select_and_classify_columns(
    all_columns: list[TableColumn],
    requested_columns: Sequence[str],
) -> ClassifiedSemiStructuredColumns | SemiStructuredColumnDoesNotExist | NoSemiStructuredColumns:
    """Select requested columns and classify by semi-structured support."""
    if requested_columns:
        requested_columns_set = set(requested_columns)
        columns_to_profile = [col for col in all_columns if col.name in requested_columns_set]
        if len(columns_to_profile) != len(requested_columns_set):
            found_columns = {col.name for col in columns_to_profile}
            not_found_columns = requested_columns_set - found_columns
            return SemiStructuredColumnDoesNotExist(
                existed_columns=columns_to_profile,
                not_existed_columns=list(not_found_columns),
            )
    else:
        columns_to_profile = all_columns

    supported_columns: list[TableColumn] = []
    unsupported_columns: list[TableColumn] = []

    for col in columns_to_profile:
        if col.data_type.normalized_type in SEMI_STRUCTURED_TYPES:
            supported_columns.append(col)
        else:
            unsupported_columns.append(col)

    if not supported_columns:
        return NoSemiStructuredColumns(unsupported_columns=unsupported_columns)

    return ClassifiedSemiStructuredColumns(
        supported_columns=supported_columns,
        unsupported_columns=unsupported_columns,
    )
