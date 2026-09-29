"""聚合读：并发、超时、部分成功、全部失败、熔断拒绝的分类断言。"""

import asyncio
import logging
import time

from tests.conftest import control, stats

logger = logging.getLogger("test.aggregate")


async def test_all_success(env):
    resp = await env["gw"].get("/aggregate/items")
    body = resp.json()
    logger.info("input=all upstreams ok | decision=%s | status=%d", body["category"], resp.status_code)
    assert resp.status_code == 200
    assert body["category"] == "success"
    assert body["upstreams_succeeded"] == 3
    assert set(body["data"]) == {"alpha", "beta", "gamma"}
    assert body["failures"] == []


async def test_partial_success_one_upstream_500(env):
    await control(env["control"], env["urls"]["beta"], mode="error", status_code=503)
    resp = await env["gw"].get("/aggregate/items")
    body = resp.json()
    logger.info("input=beta returns 503 | decision=%s | failures=%s",
                body["category"], body["failures"])
    assert resp.status_code == 200
    assert body["category"] == "partial_success"
    assert body["upstreams_succeeded"] == 2
    assert set(body["data"]) == {"alpha", "gamma"}
    (failure,) = body["failures"]
    assert failure["upstream"] == "beta"
    assert failure["kind"] == "upstream_error"
    assert failure["status_code"] == 503
    assert "503" in failure["message"]  # 可读原因，不含原始异常堆栈


async def test_partial_success_one_upstream_timeout(env):
    await control(env["control"], env["urls"]["beta"], mode="sleep", delay=2.0)
    started = time.monotonic()
    resp = await env["gw"].get("/aggregate/items")
    elapsed = time.monotonic() - started
    body = resp.json()
    logger.info("input=beta sleeps 2s (timeout=0.4s) | decision=%s | elapsed=%.2fs",
                body["category"], elapsed)
    assert resp.status_code == 200
    assert body["category"] == "partial_success"
    (failure,) = body["failures"]
    assert failure["kind"] == "timeout"
    assert elapsed < 1.5  # 没有等满 2s


async def test_aggregation_is_concurrent(env):
    for url in env["urls"].values():
        await control(env["control"], url, mode="sleep", delay=0.3)
    started = time.monotonic()
    resp = await env["gw"].get("/aggregate/items")
    elapsed = time.monotonic() - started
    body = resp.json()
    logger.info("input=all sleep 0.3s | decision=%s | elapsed=%.2fs (串行应 >=0.9s)",
                body["category"], elapsed)
    assert body["category"] == "success"
    assert elapsed < 0.75  # 并发：总耗时约等于单个上游而非三者之和


async def test_all_failed(env):
    for url in env["urls"].values():
        await control(env["control"], url, mode="error", status_code=500)
    resp = await env["gw"].get("/aggregate/items")
    body = resp.json()
    logger.info("input=all upstreams 500 | decision=%s | status=%d", body["category"], resp.status_code)
    assert resp.status_code == 503
    assert body["category"] == "all_failed"
    assert body["upstreams_succeeded"] == 0
    assert len(body["failures"]) == 3
    assert all(f["kind"] == "upstream_error" for f in body["failures"])


async def test_rejected_when_all_circuits_open(env):
    # 连续失败打满阈值，使三个上游全部熔断
    for url in env["urls"].values():
        await control(env["control"], url, mode="error", status_code=500)
    for _ in range(3):
        await env["gw"].get("/aggregate/items")
    calls_before = {}
    for name, url in env["urls"].items():
        calls_before[name] = (await stats(env["control"], url)).json()["get_items_calls"]

    resp = await env["gw"].get("/aggregate/items")
    body = resp.json()
    logger.info("input=all circuits open | decision=%s | failures=%s",
                body["category"], [f["kind"] for f in body["failures"]])
    assert resp.status_code == 503
    assert body["category"] == "rejected"
    assert all(f["kind"] == "circuit_open" for f in body["failures"])
    # 熔断期间快速失败：上游没有收到新请求
    for name, url in env["urls"].items():
        calls_after = (await stats(env["control"], url)).json()["get_items_calls"]
        assert calls_after == calls_before[name]
