"""网关业务编排：聚合读 + 幂等写。"""

from typing import Any

from .aggregator import AggregateResult, Aggregator
from .config import GatewayConfig
from .idempotency import IdempotencyStore


class GatewayService:
    def __init__(self, config: GatewayConfig, aggregator: Aggregator, idem: IdempotencyStore):
        self._config = config
        self._aggregator = aggregator
        self._idem = idem

    async def read_items(self) -> AggregateResult:  # pragma: no cover - 骨架
        raise NotImplementedError

    async def write_item(self, key: str | None, payload: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover - 骨架
        raise NotImplementedError
