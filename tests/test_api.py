"""HTTP 层测试：用 ASGI transport，不监听真实端口，上游用假实现注入。"""

import asyncio
from contextlib import asynccontextmanager

import httpx

from gateway.app import create_app
from gateway.aggregator import Aggregator
from gateway.config import CircuitConfig, IdempotencyConfig, Settings
from gateway.errors import UpstreamCallError
from gateway.idempotency import IdempotencyStore
from tests.fakes import FakeUpstream, timeout_error, circuit_open_error


@asynccontextmanager
async def harness(upstreams, *, ttl=300.0, write_action=None):
    fake_clock = [0.0]
    clock = lambda: fake_clock[0]
    store = IdempotencyStore(ttl, clock=clock)
    aggregator = Aggregator(upstreams)
    app = create_app(
        Settings(upstreams=(), circuit=CircuitConfig(),
                 idempotency=IdempotencyConfig(ttl)),
        aggregator=aggregator,
        idempotency=store,
        write_action=write_action,
        clock=clock,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
        yield client, fake_clock


async def test_aggregate_success_http():
    ups = [FakeUpstream("a"), FakeUpstream("b")]
    async with harness(ups) as (client, _):
        resp = await client.get("/v1/aggregate", params={"key": "k1"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "success"
        assert len(body["results"]) == 2


async def test_aggregate_partial_success_is_200():
    ups = [FakeUpstream("a"), FakeUpstream("b")]
    ups[1].mode = timeout_error("b")
    async with harness(ups) as (client, _):
        resp = await client.get("/v1/aggregate", params={"key": "k1"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "partial_success"


async def test_aggregate_all_failed_is_502_and_masked():
    ups = [FakeUpstream("a"), FakeUpstream("b")]
    for u in ups:
        u.mode = timeout_error(u.name)
    async with harness(ups) as (client, _):
        resp = await client.get("/v1/aggregate", params={"key": "k"})
        assert resp.status_code == 502
        body = resp.json()
        assert body["status"] == "all_failed"
        assert all("Traceback" not in (r["error"] or "") for r in body["results"])


async def test_aggregate_rejected_is_503():
    ups = [FakeUpstream("a"), FakeUpstream("b")]
    for u in ups:
        u.mode = circuit_open_error(u.name)
    async with harness(ups) as (client, _):
        resp = await client.get("/v1/aggregate", params={"key": "k"})
        assert resp.status_code == 503
        assert resp.json()["status"] == "rejected"


async def test_write_requires_idempotency_key():
    async with harness([FakeUpstream("a")]) as (client, _):
        resp = await client.post("/v1/orders", json={"item": "book"})
        assert resp.status_code == 400
        assert resp.json()["detail"]["code"] == "missing_key"


async def test_write_replay_returns_first_result():
    executions = 0

    async def action(payload):
        nonlocal executions
        executions += 1
        await asyncio.sleep(0.02)
        return 201, {"order_id": "ord-1", "item": payload["item"]}

    async with harness([FakeUpstream("a")], write_action=action) as (client, _):
        headers = {"Idempotency-Key": "key-1"}
        r1 = await client.post("/v1/orders", json={"item": "book", "qty": 1}, headers=headers)
        r2 = await client.post("/v1/orders", json={"item": "book", "qty": 1}, headers=headers)
        assert r1.status_code == r2.status_code == 201
        assert r1.json()["order_id"] == r2.json()["order_id"] == "ord-1"
        assert r1.json()["idempotent_replay"] is False
        assert r2.json()["idempotent_replay"] is True
        assert executions == 1


async def test_write_conflict_distinguished():
    async with harness([FakeUpstream("a")]) as (client, _):
        headers = {"Idempotency-Key": "key-2"}
        await client.post("/v1/orders", json={"item": "book"}, headers=headers)
        resp = await client.post("/v1/orders", json={"item": "pen"}, headers=headers)
        assert resp.status_code == 409
        assert resp.json()["detail"]["code"] == "conflict"


async def test_write_expired_distinguished():
    async with harness([FakeUpstream("a")], ttl=10.0) as (client, clock):
        headers = {"Idempotency-Key": "key-3"}
        await client.post("/v1/orders", json={"item": "book"}, headers=headers)
        clock[0] = 11.0
        resp = await client.post("/v1/orders", json={"item": "book"}, headers=headers)
        assert resp.status_code == 410
        assert resp.json()["detail"]["code"] == "expired"


async def test_concurrent_writes_execute_once():
    executions = 0

    async def slow_action(payload):
        nonlocal executions
        executions += 1
        await asyncio.sleep(0.05)
        return 201, {"order_id": "ord-only"}

    async with harness([FakeUpstream("a")], write_action=slow_action) as (client, _):
        headers = {"Idempotency-Key": "hot"}

        async def one():
            return await client.post("/v1/orders", json={"item": "x"}, headers=headers)

        responses = await asyncio.gather(*(one() for _ in range(6)))
        assert executions == 1
        assert all(r.status_code == 201 for r in responses)
        bodies = [r.json() for r in responses]
        assert sum(1 for b in bodies if b["idempotent_replay"] is False) == 1
        assert sum(1 for b in bodies if b["idempotent_replay"] is True) == 5


async def test_failed_write_does_not_cache_and_retry_succeeds():
    flips = {"n": 0}

    async def flaky_action(payload):
        flips["n"] += 1
        if flips["n"] == 1:
            raise UpstreamCallError("上游[write]暂时不可达（连接失败）")
        return 201, {"order_id": "ord-retry"}

    async with harness([FakeUpstream("a")], write_action=flaky_action) as (client, _):
        headers = {"Idempotency-Key": "key-fail"}
        r1 = await client.post("/v1/orders", json={"item": "x"}, headers=headers)
        assert r1.status_code == 502
        r2 = await client.post("/v1/orders", json={"item": "x"}, headers=headers)
        assert r2.status_code == 201
        assert r2.json()["order_id"] == "ord-retry"
