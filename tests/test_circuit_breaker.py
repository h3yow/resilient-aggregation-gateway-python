"""熔断器单元测试：进入、快速失败、冷却探测、恢复、抖动免疫、并发单探测。"""

import asyncio
import logging

import pytest

from gateway.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState
from gateway.config import UpstreamConfig

logger = logging.getLogger("test.circuit_breaker")


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


def make_breaker(clock, **overrides) -> CircuitBreaker:
    params = dict(name="u", base_url="http://x", failure_threshold=3,
                  recovery_timeout=5.0, half_open_probes=1, success_threshold=1)
    params.update(overrides)
    return CircuitBreaker(UpstreamConfig(**params), clock=clock)


async def fail():
    raise RuntimeError("boom")


async def ok():
    return "fine"


async def test_opens_after_threshold_and_fast_fails():
    clock = FakeClock()
    cb = make_breaker(clock)
    for i in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(fail)
        logger.info("input=failure #%d | state=%s", i + 1, cb.state)
    assert cb.state == CircuitState.OPEN
    with pytest.raises(CircuitOpenError) as exc_info:
        await cb.call(ok)
    logger.info("input=call during open | branch=reject retry_after=%.3f", exc_info.value.retry_after)
    assert exc_info.value.retry_after > 0


async def test_flapping_does_not_trip():
    clock = FakeClock()
    cb = make_breaker(clock)
    for round_ in range(5):
        with pytest.raises(RuntimeError):
            await cb.call(fail)
        with pytest.raises(RuntimeError):
            await cb.call(fail)
        await cb.call(ok)  # 成功重置连续失败计数
        logger.info("input=fail,fail,ok round=%d | state=%s", round_, cb.state)
    assert cb.state == CircuitState.CLOSED


async def test_cooldown_then_probe_recovers():
    clock = FakeClock()
    cb = make_breaker(clock)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(fail)
    clock.advance(4.9)
    with pytest.raises(CircuitOpenError):
        await cb.call(ok)
    logger.info("input=call at 4.9s (< cooldown 5s) | branch=reject state=%s", cb.state)
    clock.advance(0.2)
    assert await cb.call(ok) == "fine"
    logger.info("input=probe at 5.1s | branch=close state=%s", cb.state)
    assert cb.state == CircuitState.CLOSED


async def test_probe_failure_reopens():
    clock = FakeClock()
    cb = make_breaker(clock)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(fail)
    clock.advance(5.0)
    with pytest.raises(RuntimeError):
        await cb.call(fail)  # 探测失败 -> 重新打开
    logger.info("input=failed probe | branch=reopen state=%s", cb.state)
    assert cb.state == CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        await cb.call(ok)


async def test_success_threshold_requires_multiple_probes():
    clock = FakeClock()
    cb = make_breaker(clock, success_threshold=2)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(fail)
    clock.advance(5.0)
    await cb.call(ok)
    assert cb.state == CircuitState.HALF_OPEN  # 第一次探测成功还不够
    await cb.call(ok)
    logger.info("input=2 successful probes | branch=close state=%s", cb.state)
    assert cb.state == CircuitState.CLOSED


async def test_concurrent_single_probe_no_stampede():
    clock = FakeClock()
    cb = make_breaker(clock, half_open_probes=1)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(fail)
    clock.advance(5.0)

    entered = 0
    release = asyncio.Event()

    async def slow_probe():
        nonlocal entered
        entered += 1
        await release.wait()
        return "done"

    probe_task = asyncio.create_task(cb.call(slow_probe))
    await asyncio.sleep(0)  # 让探测先进入
    results = await asyncio.gather(
        *(cb.call(ok) for _ in range(5)), return_exceptions=True
    )
    rejected = [r for r in results if isinstance(r, CircuitOpenError)]
    logger.info("input=5 concurrent calls during probe | executed=%d rejected=%d",
                entered, len(rejected))
    assert entered == 1            # 只有一个探测真正执行
    assert len(rejected) == 5      # 其余全部快速失败，无重复探测
    release.set()
    assert await probe_task == "done"
    assert cb.state == CircuitState.CLOSED


async def test_no_early_release_during_open():
    clock = FakeClock()
    cb = make_breaker(clock)
    executed = 0

    async def counting():
        nonlocal executed
        executed += 1
        raise RuntimeError("boom")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(counting)
    for _ in range(10):
        with pytest.raises(CircuitOpenError):
            await cb.call(counting)
    logger.info("input=10 calls during open | executed_total=%d (阈值 3 次后不再放行)", executed)
    assert executed == 3
