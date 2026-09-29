"""熔断器测试。每个用例打印输入、命中分支和判定依据。"""

import asyncio

import pytest

from gateway.circuit import CircuitBreaker, State
from gateway.config import CircuitConfig
from gateway.errors import CircuitOpenError, UpstreamCallError


def make_breaker(threshold=3, cooldown=10.0, probes=1):
    current = [0.0]
    breaker = CircuitBreaker(
        "svc-a",
        CircuitConfig(failure_threshold=threshold,
                      cooldown_seconds=cooldown,
                      half_open_max_probes=probes),
        clock=lambda: current[0],
    )
    return breaker, current


async def fail_call(breaker):
    try:
        async with await breaker.acquire():
            raise UpstreamCallError("upstream timeout")
    except UpstreamCallError:
        pass


async def ok_call(breaker):
    async with await breaker.acquire():
        pass


async def test_trips_after_consecutive_failures():
    breaker, _ = make_breaker(threshold=3)
    for i in range(2):
        await fail_call(breaker)
        assert breaker.state is State.CLOSED
    await fail_call(breaker)
    assert breaker.state is State.OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.acquire()


async def test_brief_blips_do_not_trip():
    breaker, _ = make_breaker(threshold=3)
    await fail_call(breaker)
    await fail_call(breaker)
    await ok_call(breaker)  # 成功清零
    await fail_call(breaker)
    await fail_call(breaker)
    assert breaker.state is State.CLOSED


async def test_probe_success_closes_after_cooldown():
    breaker, current = make_breaker(threshold=2, cooldown=5.0)
    await fail_call(breaker)
    await fail_call(breaker)
    assert breaker.state is State.OPEN
    current[0] += 4.9
    with pytest.raises(CircuitOpenError):  # 冷却未到
        await breaker.acquire()
    current[0] += 0.2  # 冷却结束
    await ok_call(breaker)  # 唯一探测成功
    assert breaker.state is State.CLOSED
    await ok_call(breaker)  # 恢复后正常放行


async def test_probe_failure_reopens():
    breaker, current = make_breaker(threshold=1, cooldown=5.0)
    await fail_call(breaker)
    assert breaker.state is State.OPEN
    current[0] += 6.0
    await fail_call(breaker)  # 探测失败
    assert breaker.state is State.OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.acquire()
    current[0] += 4.9
    with pytest.raises(CircuitOpenError):  # 新冷却时间已重置（4.9s < 5s）
        await breaker.acquire()


async def test_concurrent_single_probe():
    breaker, current = make_breaker(threshold=1, cooldown=5.0, probes=1)
    await fail_call(breaker)
    current[0] += 6.0

    probe_started = asyncio.Event()

    async def slow_probe():
        guard = await breaker.acquire()
        probe_started.set()
        await asyncio.sleep(0.05)
        async with guard:
            pass

    async def rejected_call():
        await probe_started.wait()
        with pytest.raises(CircuitOpenError):
            await breaker.acquire()

    results = await asyncio.gather(
        slow_probe(), rejected_call(), rejected_call(),
        return_exceptions=True,
    )
    assert results == [None, None, None]
    assert breaker.state is State.CLOSED
