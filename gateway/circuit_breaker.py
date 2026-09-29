"""每上游熔断器：closed / open / half_open 三态。"""

import enum
from typing import Any, Callable, Optional

from .config import Clock, UpstreamConfig


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """熔断打开期间请求被提前拒绝时抛出。"""

    def __init__(self, name: str, retry_after: float):
        super().__init__(f"circuit open for upstream {name!r}")
        self.name = name
        self.retry_after = retry_after


class CircuitBreaker:
    """单个上游的熔断器（仅内存状态，锁保护并发）。"""

    def __init__(self, config: UpstreamConfig, clock: Clock):
        self.config = config
        self._clock = clock

    async def call(self, operation):  # pragma: no cover - 骨架
        raise NotImplementedError

    def snapshot(self) -> dict[str, Any]:  # pragma: no cover - 骨架
        raise NotImplementedError
