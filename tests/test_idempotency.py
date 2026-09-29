"""幂等写：缺失/冲突/过期拒绝、重复提交去重、并发单飞、失败不占键。"""

import asyncio
import logging

import httpx
import pytest_asyncio

from gateway.config import GatewayConfig, UpstreamConfig
from gateway.main import create_app
from gateway.service import GatewayService
from tests.conftest import NAMES, control, stats

logger = logging.getLogger("test.idempotency")

PAYLOAD = {"name": "widget", "price": 42}


async def _write_calls(env) -> int:
    s = await stats(env["control"], env["urls"][NAMES[0]])
    return s.json()["post_items_calls"]


async def test_missing_key_rejected(env):
    resp = await env["gw"].post("/items", json=PAYLOAD)
    body = resp.json()
    logger.info("input=no Idempotency-Key | status=%d code=%s", resp.status_code, body["error"]["code"])
    assert resp.status_code == 400
    assert body["error"]["code"] == "key_missing"
    assert await _write_calls(env) == 0


async def test_replay_returns_first_result_without_reexecuting(env):
    r1 = await env["gw"].post("/items", json=PAYLOAD, headers={"Idempotency-Key": "k-1"})
    r2 = await env["gw"].post("/items", json=PAYLOAD, headers={"Idempotency-Key": "k-1"})
    b1, b2 = r1.json(), r2.json()
    logger.info("input=same key+body twice | replayed=%s/%s upstream_seq=%s/%s",
                b1["idempotent_replay"], b2["idempotent_replay"], b1["seq"], b2["seq"])
    assert r1.status_code == r2.status_code == 200
    assert b1["idempotent_replay"] is False
    assert b2["idempotent_replay"] is True
    assert b1["seq"] == b2["seq"]            # 返回首次结果
    assert await _write_calls(env) == 1      # 上游只执行一次


async def test_conflicting_payload_rejected(env):
    await env["gw"].post("/items", json=PAYLOAD, headers={"Idempotency-Key": "k-2"})
    resp = await env["gw"].post("/items", json={"name": "other"},
                                headers={"Idempotency-Key": "k-2"})
    body = resp.json()
    logger.info("input=same key different body | status=%d code=%s",
                resp.status_code, body["error"]["code"])
    assert resp.status_code == 409
    assert body["error"]["code"] == "key_conflict"


async def test_concurrent_same_key_executes_once(env):
    keys = {"Idempotency-Key": "k-concurrent"}
    responses = await asyncio.gather(*(
        env["gw"].post("/items", json=PAYLOAD, headers=keys) for _ in range(10)
    ))
    bodies = [r.json() for r in responses]
    replays = [b["idempotent_replay"] for b in bodies]
    seqs = {b["seq"] for b in bodies}
    logger.info("input=10 concurrent same-key writes | executed=%d replayed=%d distinct_seq=%d",
                replays.count(False), replays.count(True), len(seqs))
    assert all(r.status_code == 200 for r in responses)
    assert replays.count(False) == 1         # 只有一个真正执行
    assert replays.count(True) == 9
    assert len(seqs) == 1                    # 全部拿到同一个首次结果
    assert await _write_calls(env) == 1


async def test_upstream_failure_does_not_burn_key(env):
    write_url = env["urls"][NAMES[0]]
    await control(env["control"], write_url, mode="error", status_code=500)
    r1 = await env["gw"].post("/items", json=PAYLOAD, headers={"Idempotency-Key": "k-3"})
    logger.info("input=write upstream 500 | status=%d code=%s", r1.status_code, r1.json()["error"]["code"])
    assert r1.status_code == 502
    assert r1.json()["error"]["code"] == "upstream_write_failed"
    await control(env["control"], write_url, mode="ok")
    r2 = await env["gw"].post("/items", json=PAYLOAD, headers={"Idempotency-Key": "k-3"})
    logger.info("input=retry same key after recovery | status=%d replayed=%s",
                r2.status_code, r2.json()["idempotent_replay"])
    assert r2.status_code == 200
    assert r2.json()["idempotent_replay"] is False  # 失败不占键，可重试


@pytest_asyncio.fixture
async def short_ttl_env(upstream_urls):
    upstreams = tuple(
        UpstreamConfig(name=n, base_url=upstream_urls[n], timeout=0.4)
        for n in NAMES
    )
    service = GatewayService(GatewayConfig(upstreams=upstreams, idempotency_ttl=0.2))
    app = create_app(service)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gateway") as gw, \
            httpx.AsyncClient() as ctrl:
        yield {"gw": gw, "control": ctrl}
        await service.aclose()


async def test_expired_key_rejected(short_ttl_env):
    gw = short_ttl_env["gw"]
    r1 = await gw.post("/items", json=PAYLOAD, headers={"Idempotency-Key": "k-ttl"})
    assert r1.status_code == 200
    await asyncio.sleep(0.35)  # 超过 TTL 0.2s
    r2 = await gw.post("/items", json=PAYLOAD, headers={"Idempotency-Key": "k-ttl"})
    body = r2.json()
    logger.info("input=same key after ttl=0.2s | status=%d code=%s",
                r2.status_code, body["error"]["code"])
    assert r2.status_code == 410
    assert body["error"]["code"] == "key_expired"
