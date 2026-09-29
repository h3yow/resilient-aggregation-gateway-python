"""聚合读测试：全成功、部分成功、全失败、全部被提前拒绝。"""

import asyncio
import time

from gateway.aggregator import Aggregator, AggregateStatus
from tests.fakes import FakeUpstream, timeout_error, circuit_open_error


def make(three=()):
    upstreams = [FakeUpstream("a"), FakeUpstream("b"), FakeUpstream("c")]
    return Aggregator(upstreams), upstreams


async def test_all_success():
    agg, ups = make()
    resp = await agg.query_all("k1")
    assert resp.status is AggregateStatus.SUCCESS
    assert all(r.ok for r in resp.results)
    assert [r.name for r in resp.results] == ["a", "b", "c"]


async def test_partial_success_masks_raw_exception():
    agg, ups = make()
    ups[1].mode = timeout_error("b")
    ups[2].mode = circuit_open_error("c")
    resp = await agg.query_all("k1")
    assert resp.status is AggregateStatus.PARTIAL_SUCCESS
    assert resp.results[1].error and "超时" in resp.results[1].error
    assert resp.results[2].rejected is True
    assert "1/3" in (resp.reason or "")


async def test_all_failed_vs_rejected():
    agg1, ups1 = make()
    for u in ups1:
        u.mode = timeout_error(u.name)
    resp = await agg1.query_all("k1")
    assert resp.status is AggregateStatus.ALL_FAILED
    assert all(r.rejected is False for r in resp.results)

    agg2, ups2 = make()
    for u in ups2:
        u.mode = circuit_open_error(u.name)
    resp2 = await agg2.query_all("k1")
    assert resp2.status is AggregateStatus.REJECTED
    assert all(r.rejected for r in resp2.results)


async def test_mixed_failures_without_success_is_rejected():
    # 没有成功 + 至少一个提前拒绝 => rejected
    agg, ups = make()
    ups[0].mode = timeout_error("a")
    ups[1].mode = timeout_error("b")
    ups[2].mode = circuit_open_error("c")
    resp = await agg.query_all("k1")
    assert resp.status is AggregateStatus.REJECTED


async def test_empty_key_rejected_without_calls():
    agg, ups = make()
    resp = await agg.query_all("   ")
    assert resp.status is AggregateStatus.REJECTED
    assert all(u.calls == 0 for u in ups)


async def test_calls_are_concurrent():
    upstreams = [FakeUpstream(f"u{i}", delay=0.05) for i in range(5)]
    agg = Aggregator(upstreams)
    start = time.monotonic()
    resp = await agg.query_all("k")
    elapsed = time.monotonic() - start
    assert resp.status is AggregateStatus.SUCCESS
    assert elapsed < 0.2  # 串行约需 0.25s，并发应远小于它


async def test_hang_aggregator_isolation():
    # 一个挂死的上游不能拖垮整体（模拟超时后取消）
    async def slow_fail():
        await asyncio.sleep(0.02)
        raise timeout_error("slow")

    class SlowFake(FakeUpstream):
        async def get(self, path, params=None):
            self.calls += 1
            return await slow_fail()

    good = FakeUpstream("good")
    bad = SlowFake("bad")
    agg = Aggregator([good, bad])
    resp = await agg.query_all("k")
    assert resp.status is AggregateStatus.PARTIAL_SUCCESS
