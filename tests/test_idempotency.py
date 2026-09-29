"""幂等存储测试：缺键、冲突、过期、重复提交、并发只执行一次。"""

import asyncio

import pytest

from gateway.idempotency import (
    IdempotencyStore,
    IdempotencyRejected,
    RejectReason,
    StoredResponse,
    hash_request,
)


def make_store(ttl=300.0):
    current = [0.0]
    store = IdempotencyStore(ttl, clock=lambda: current[0])
    return store, current


def resp(now=0.0):
    return StoredResponse(status_code=201, body={"order": "x-1"}, created_at=now)


async def test_missing_key_rejected():
    store, _ = make_store()
    with pytest.raises(IdempotencyRejected) as ei:
        await store.begin("   ", "h")
    assert ei.value.reason is RejectReason.MISSING_KEY


async def test_repeat_returns_first_result_without_rerun():
    store, current = make_store()
    executions = 0

    async def execute(key, payload):
        nonlocal executions
        stored = await store.begin(key, hash_request(payload))
        if stored is not None:
            return stored
        executions += 1
        await asyncio.sleep(0.02)  # 模拟处理耗时
        result = resp(current[0])
        await store.complete(key, result)
        return result

    payload = {"item": "book", "qty": 1}
    first = await execute("k-1", payload)
    second = await execute("k-1", payload)
    third = await execute("k-1", payload)
    assert executions == 1
    assert first.body == second.body == third.body == {"order": "x-1"}


async def test_conflicting_payload_rejected():
    store, current = make_store()
    assert await store.begin("k-9", hash_request({"a": 1})) is None
    await store.complete("k-9", resp())
    with pytest.raises(IdempotencyRejected) as ei:
        await store.begin("k-9", hash_request({"a": 2}))
    assert ei.value.reason is RejectReason.CONFLICT


async def test_expired_record_rejected():
    store, current = make_store(ttl=10.0)
    assert await store.begin("k-exp", hash_request({"a": 1})) is None
    await store.complete("k-exp", StoredResponse(201, {"order": "z"}, 0.0))
    current[0] = 9.9
    stored = await store.begin("k-exp", hash_request({"a": 1}))
    assert stored.body == {"order": "z"}
    current[0] = 10.1
    with pytest.raises(IdempotencyRejected) as ei:
        await store.begin("k-exp", hash_request({"a": 1}))
    assert ei.value.reason is RejectReason.EXPIRED


async def test_concurrent_same_key_executes_once():
    store, current = make_store()
    executions = 0
    started = asyncio.Event()
    results = []

    async def execute():
        nonlocal executions
        stored = await store.begin("hot-key", hash_request({"x": 1}))
        if stored is not None:
            results.append(stored)
            return
        executions += 1
        started.set()
        await asyncio.sleep(0.05)
        out = StoredResponse(201, {"order": "only-one"}, current[0])
        await store.complete("hot-key", out)
        results.append(out)

    await asyncio.gather(*(execute() for _ in range(8)))
    assert executions == 1
    assert len(results) == 8
    assert all(r.body == {"order": "only-one"} for r in results)


async def test_failed_execution_allows_retry():
    store, _ = make_store()
    assert await store.begin("k-fail", hash_request({"x": 1})) is None
    await store.abort("k-fail")
    # 重试拿到执行权
    assert await store.begin("k-fail", hash_request({"x": 1})) is None
