"""UpstreamClient 与真实 httpx 传输层（MockTransport）+ 熔断器的集成测试。"""

import httpx
import pytest

from gateway.circuit import State
from gateway.config import CircuitConfig, UpstreamConfig
from gateway.errors import UpstreamCallError
from gateway.upstream import UpstreamClient


def make_client(handler):
    transport = httpx.MockTransport(handler)
    breaker_holder = {}
    cfg = UpstreamConfig(name="svc", base_url="http://up", timeout=1.0)

    from gateway.circuit import CircuitBreaker
    breaker = CircuitBreaker("svc", CircuitConfig(failure_threshold=2, cooldown_seconds=10))
    client = UpstreamClient(cfg, breaker, transport=transport)
    return client, breaker


async def test_http_500_counts_toward_circuit():
    def handler(request):
        return httpx.Response(500, json={"error": "boom"})

    client, breaker = make_client(handler)
    for _ in range(2):
        with pytest.raises(UpstreamCallError) as ei:
            await client.get("/items")
        assert "HTTP 500" in str(ei.value)
        assert "boom" not in str(ei.value)  # 原始上游信息不透出
    assert breaker.state is State.OPEN


async def test_timeout_is_masked():
    def handler(request):
        raise httpx.TimeoutException("deadline exceeded internally")

    client, breaker = make_client(handler)
    with pytest.raises(UpstreamCallError) as ei:
        await client.get("/items")
    assert "超时" in str(ei.value)
    assert "deadline exceeded" not in str(ei.value)
