"""熔断器：闭合 -> 打开 -> 半开探测 -> 恢复。

规则：
- 连续失败达到阈值才打开（成功会清零计数，短暂抖动不触发）。
- 打开期间所有请求快速失败，不访问上游。
- 冷却结束后转为半开，只放行有限数量的探测请求，其余仍快速拒绝。
- 探测成功且在途探测全部结束 -> 闭合；探测失败 -> 立即重新打开并重置冷却。
"""

import asyncio
import enum
import logging
import time
from typing import Callable

from .config import CircuitConfig
from .errors import CircuitOpenError, UpstreamCallError

logger = logging.getLogger("gateway.circuit")


class State(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class _Guard:
    """单次上游调用的上下文，记录该调用是以探测身份放行的。"""

    def __init__(self, breaker: "CircuitBreaker", probe: bool) -> None:
        self._breaker = breaker
        self._probe = probe

    async def __aenter__(self) -> "_Guard":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        failure = exc_type is not None and issubclass(exc_type, UpstreamCallError)
        await self._breaker._record(failure, self._probe)
        return False


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        config: CircuitConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.config = config
        self._clock = clock
        self._state = State.CLOSED
        self._failure_count = 0
        self._opened_at = 0.0
        self._active_probes = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> State:
        return self._state

    async def acquire(self) -> _Guard:
        """申请调用许可；不允许时抛 CircuitOpenError。"""
        async with self._lock:
            now = self._clock()
            if self._state is State.CLOSED:
                logger.info("breaker=%s 命中分支=闭合放行 判定依据=连续失败=%d/%d",
                            self.name, self._failure_count, self.config.failure_threshold)
                return _Guard(self, False)

            if self._state is State.OPEN:
                elapsed = now - self._opened_at
                if elapsed < self.config.cooldown_seconds:
                    reason = (f"熔断器[{self.name}]打开中，"
                              f"冷却剩余{self.config.cooldown_seconds - elapsed:.2f}s，快速拒绝")
                    logger.info("breaker=%s 命中分支=打开拒绝 判定依据=冷却未到 elapsed=%.2fs",
                                self.name, elapsed)
                    raise CircuitOpenError(reason)
                # 冷却结束，尝试进入半开放行探测
                if self._active_probes >= self.config.half_open_max_probes:
                    logger.info("breaker=%s 命中分支=半开拒绝 判定依据=探测名额已满 probes=%d",
                                self.name, self._active_probes)
                    raise CircuitOpenError(
                        f"熔断器[{self.name}]半开探测中，已有请求在试探恢复，快速拒绝")
                self._state = State.HALF_OPEN
                self._active_probes += 1
                logger.info("breaker=%s 命中分支=冷却结束转入半开 判定依据=放行探测 probes=%d",
                            self.name, self._active_probes)
                return _Guard(self, True)

            # HALF_OPEN：探测名额有限，防止并发重复探测
            if self._active_probes >= self.config.half_open_max_probes:
                logger.info("breaker=%s 命中分支=半开拒绝 判定依据=探测名额已满 probes=%d",
                            self.name, self._active_probes)
                raise CircuitOpenError(
                    f"熔断器[{self.name}]半开探测中，已有请求在试探恢复，快速拒绝")
            self._active_probes += 1
            logger.info("breaker=%s 命中分支=半开放行 判定依据=额外探测 probes=%d",
                        self.name, self._active_probes)
            return _Guard(self, True)

    async def _record(self, failure: bool, was_probe: bool) -> None:
        async with self._lock:
            now = self._clock()
            if was_probe:
                self._active_probes = max(0, self._active_probes - 1)
                if failure:
                    self._trip(now)
                    logger.info("breaker=%s 命中分支=探测失败重新打开 判定依据=上游仍异常",
                                self.name)
                elif self._state is State.HALF_OPEN and self._active_probes == 0:
                    self._state = State.CLOSED
                    self._failure_count = 0
                    logger.info("breaker=%s 命中分支=探测成功恢复闭合 判定依据=上游已恢复",
                                self.name)
                else:
                    # 其他探测已先行失败并重新打开：保持打开，等待新一轮冷却
                    logger.info("breaker=%s 命中分支=探测结束但熔断保持打开 判定依据=存在失败探测 probes=%d",
                                self.name, self._active_probes)
                return

            # 以非探测身份放行的调用，或并发探测已先行恢复的情况，按闭合规则计数
            if failure:
                self._failure_count += 1
                logger.info("breaker=%s 命中分支=记录失败 判定依据=连续失败=%d/%d",
                            self.name, self._failure_count, self.config.failure_threshold)
                if self._state is State.CLOSED and self._failure_count >= self.config.failure_threshold:
                    self._trip(now)
            elif self._state is State.CLOSED:
                if self._failure_count:
                    logger.info("breaker=%s 命中分支=成功清零计数 判定依据=调用成功", self.name)
                self._failure_count = 0

    def _trip(self, now: float) -> None:
        self._state = State.OPEN
        self._opened_at = now
        self._active_probes = 0
        logger.info("breaker=%s 命中分支=熔断打开 判定依据=连续失败达到阈值=%d",
                    self.name, self.config.failure_threshold)
