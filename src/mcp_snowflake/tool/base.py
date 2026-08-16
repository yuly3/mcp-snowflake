from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

import mcp.types as types


class Tool(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> list[types.ContentBlock]: ...

    @property
    @abstractmethod
    def definition(self) -> types.Tool: ...
