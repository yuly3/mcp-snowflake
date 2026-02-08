from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel, Field


class ContainsNameFilter(BaseModel):
    type_: Literal["contains"] = Field(alias="type")
    value: str = Field(min_length=1)


type NameFilter = ContainsNameFilter


def apply_name_filter(names: Sequence[str], name_filter: NameFilter | None) -> list[str]:
    if name_filter is None:
        return list(names)

    query = name_filter.value.casefold()
    return [name for name in names if query in name.casefold()]
