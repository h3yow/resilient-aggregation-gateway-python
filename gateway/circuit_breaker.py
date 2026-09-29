"""每上游熔断器：closed / open / half_open 三态。

- closed：正常放行；连续失败达到 failure_threshold 进入 open。
- open：快速失败（抛 CircuitOpenError）；冷却 recovery_timeout 后进入 half_open。
- half_open：最多放行 half_open_probes 个并发探测，其余快速失败；
  探测连续成功 success_threshold 次回到 closed，任一探测失败立即回到 open。

短暂抖动（失败穿插成功）会重置连续失败计数，不会误触发熔断。
"""

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

from .config import Clock, UpstreamConfig

logger = logging.getLogger("gateway.circuit_breaker")


class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """熔断打开期间请求被提前拒绝。"""

    def __init__(self, name: str, retry_after: float):
        super().__init__(f"circuit open for upstream {name!r}, retry after {retry_after:.3f}s")
        self.name = name
        self.retry_after = retry_after


class CircuitBreaker:
    """单个上游的熔断器（内存状态，asyncio.Lock 保护并发）。"""

    def __init__(self, config: UpstreamConfig, clock: Clock = time.monotonic):
        self.config = config
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._opened_at = 0.0
        self._in_flight_probes = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> str:
        return self._state

    async def call(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        """放行 / 拒绝一次调用，并根据结果驱动状态迁移。"""
        is_probe = await self._admit()
        try:
            result = await operation()
        except Exception:
            await self._on_failure(is_probe)
            raise
        await self._on_success(is_probe)
        return result

    async def _admit(self) -> bool:
        """返回本次调用是否为探测请求；拒绝时抛 CircuitOpenError。"""
        async with self._lock:
            now = self._clock()
            if self._state == CircuitState.OPEN:
                elapsed = now - self._opened_at
                if elapsed < self.config.recovery_timeout:
                    retry_after = self.config.recovery_timeout - elapsed
                    logger.info(
                        "upstream=%s branch=reject reason=circuit_open retry_after=%.3fs",
                        self.config.name, retry_after,
                    )
                    raise CircuitOpenError(self.config.name, retry_after)
                self._state = CircuitState.HALF_OPEN
                self._in_flight_probes = 0
                self._consecutive_successes = 0
                logger.info(
                    "upstream=%s branch=half_open reason=cooldown_elapsed elapsed=%.3fs",
                    self.config.name, elapsed,
                )
            if self._state == CircuitState.HALF_OPEN:
                if self._in_flight_probes >= self.config.half_open_probes:
                    logger.info(
                        "upstream=%s branch=reject reason=probe_limit in_flight=%d limit=%d",
                        self.config.name, self._in_flight_probes, self.config.half_open_probes,
                    )
                    raise CircuitOpenError(self.config.name, 0.0)
                self._in_flight_probes += 1
                logger.info(
                    "upstream=%s branch=probe reason=half_open_admit in_flight=%d",
                    self.config.name, self._in_flight_probes,
                )
                return True
            return False

    async def _on_success(self, is_probe: bool) -> None:
        async with self._lock:
            if is_probe:
                self._in_flight_probes = max(0, self._in_flight_probes - 1)
                if self._state == CircuitState.HALF_OPEN:
                    self._consecutive_successes += 1
                    if self._consecutive_successes >= self.config.success_threshold:
                        self._state = CircuitState.CLOSED
                        self._consecutive_failures = 0
                        self._consecutive_successes = 0
                        logger.info(
                            "upstream=%s branch=close reason=probe_succeeded",
                            self.config.name,
                        )
            else:
                if self._consecutive_failures:
                    logger.info(
                        "upstream=%s branch=reset_failures reason=success streak=%d",
                        self.config.name, self._consecutive_failures,
                    )
                self._consecutive_failures = 0

    async def _on_failure(self, is_probe: bool) -> None:
        async with self._lock:
            if is_probe:
                self._in_flight_probes = max(0, self._in_flight_probes - 1)
                if self._state == CircuitState.HALF_OPEN:
                    self._state = CircuitState.OPEN
                    self._opened_at = self._clock()
                    self._consecutive_successes = 0
                    logger.info(
                        "upstream=%s branch=reopen reason=probe_failed",
                        self.config.name,
                    )
                return
            self._consecutive_failures += 1
            if (
                self._state == CircuitState.CLOSED
                and self._consecutive_failures >= self.config.failure_threshold
            ):
                self._state = CircuitState.OPEN
                self._opened_at = self._clock()
                logger.info(
                    "upstream=%s branch=open reason=threshold_reached failures=%d threshold=%d",
                    self.config.name, self._consecutive_failures, self.config.failure_threshold,
                )

    def snapshot(self) -> dict[str, Any]:
        """供 /healthz 与测试断言使用的状态快照。"""
        retry_after = 0.0
        if self._state == CircuitState.OPEN:
            retry_after = max(0.0, self.config.recovery_timeout - (self._clock() - self._opened_at))
        return {
            "state": self._state,
            "consecutive_failures": self._consecutive_failures,
            "in_flight_probes": self._in_flight_probes,
            "retry_after": round(retry_after, 3),
        }
