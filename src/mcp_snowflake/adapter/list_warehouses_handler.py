"""ListWarehouses EffectHandler implementation."""

import logging
from datetime import timedelta

from ..handler.list_warehouses import WarehouseInfoDict
from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class ListWarehousesEffectHandler:
    """EffectHandler for ListWarehouses operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def list_warehouses(self) -> list[WarehouseInfoDict]:
        """Get list of available warehouses."""
        query = "SHOW WAREHOUSES"

        try:
            results = await self.client.execute_query(query, timedelta(seconds=10))
        except Exception:
            logger.exception("failed to execute list warehouses operation")
            raise

        warehouses: list[WarehouseInfoDict] = []
        for row in results:
            warehouses.append(
                WarehouseInfoDict(
                    name=row.get("name", ""),
                    state=row.get("state"),
                    size=row.get("size"),
                    owner=row.get("owner"),
                    comment=row.get("comment"),
                )
            )

        return sorted(warehouses, key=lambda w: w["name"])
