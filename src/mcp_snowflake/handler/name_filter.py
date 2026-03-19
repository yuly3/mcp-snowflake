from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel, Field

from kernel.table_metadata import ObjectKind, SchemaObject


class ContainsNameFilter(BaseModel):
    type_: Literal["contains"] = Field(alias="type")
    value: str = Field(min_length=1)


class ObjectTypeFilter(BaseModel):
    type_: Literal["object_type"] = Field(alias="type")
    value: Literal["TABLE", "VIEW"]


type ListObjectsFilter = ContainsNameFilter | ObjectTypeFilter


def apply_list_objects_filter(
    objects: Sequence[SchemaObject],
    filter_: ListObjectsFilter | None,
) -> list[SchemaObject]:
    if filter_ is None:
        return list(objects)

    match filter_:
        case ContainsNameFilter():
            query = filter_.value.casefold()
            return [obj for obj in objects if query in obj.name.casefold()]
        case ObjectTypeFilter():
            target_kind = ObjectKind(filter_.value)
            return [obj for obj in objects if obj.kind == target_kind]
