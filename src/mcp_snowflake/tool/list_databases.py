from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from expression.contract import ContractViolationError

from ..handler import (
    CompactListDatabasesResultSerializer,
    EffectListDatabases,
    handle_list_databases,
)
from .base import Tool


class ListDatabasesTool(Tool):
    def __init__(self, effect_handler: EffectListDatabases) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "list_databases"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,  # noqa: ARG002
    ) -> Sequence[types.Content]:
        try:
            result = await handle_list_databases(self.effect_handler)
        except TimeoutError as e:
            text = f"Error: Query timed out: {e}"
        except ProgrammingError as e:
            text = f"Error: SQL syntax error or other programming error: {e}"
        except OperationalError as e:
            text = f"Error: Database operation related error: {e}"
        except DataError as e:
            text = f"Error: Data processing related error: {e}"
        except IntegrityError as e:
            text = f"Error: Referential integrity constraint violation: {e}"
        except NotSupportedError as e:
            text = f"Error: Unsupported database feature used: {e}"
        except ContractViolationError as e:
            text = f"Error: Unexpected error: {e}"
        else:
            text = result.serialize_with(CompactListDatabasesResultSerializer())
        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Retrieve a list of accessible databases",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        )
