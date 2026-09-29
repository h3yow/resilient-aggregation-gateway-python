"""网关业务编排：聚合读 + 幂等写。"""

import hashlib
import json
import logging
from typing import Any

import httpx

from .aggregator import AggregateResult, Aggregator
from .circuit_breaker import CircuitBreaker
from .config import Clock, GatewayConfig
from .idempotency import IdempotencyStore

logger = logging.getLogger("gateway.service")


def fingerprint_of(payload: dict[str, Any]) -> str:
    """请求体的稳定指纹（规范 JSON 的 sha256），用于冲突检测。"""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class GatewayService:
    def __init__(
        self,
        config: GatewayConfig,
        clock: Clock | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        import time
        self._config = config
        clock = clock or time.monotonic
        self._breakers = {u.name: CircuitBreaker(u, clock=clock) for u in config.upstreams}
        self._client = client or httpx.AsyncClient()
        self._aggregator = Aggregator(config, self._breakers, self._client)
        self._idem = IdempotencyStore(config.idempotency_ttl, clock=clock,
                                      key_max_len=config.idempotency_key_max_len)

    @property
    def breakers(self) -> dict[str, CircuitBreaker]:
        return self._breakers

    @property
    def idempotency(self) -> IdempotencyStore:
        return self._idem

    async def aclose(self) -> None:
        await self._client.aclose()

    async def read_items(self) -> AggregateResult:
        logger.info("endpoint=read_items branch=start upstreams=%d", len(self._config.upstreams))
        return await self._aggregator.aggregate()

    async def write_item(self, key: str | None, payload: dict[str, Any]) -> dict[str, Any]:
        fp = fingerprint_of(payload)
        logger.info("endpoint=write_item branch=start key=%s fingerprint=%s payload_keys=%s",
                    key, fp[:12], list(payload))

        async def execute() -> dict[str, Any]:
            up = self._config.write_upstream
            try:
                resp = await self._client.post(f"{up.base_url}/items", json=payload,
                                               timeout=up.timeout)
            except httpx.TimeoutException:
                raise UpstreamWriteError(f"write upstream timed out after {up.timeout:.3f}s")
            except httpx.TransportError:
                raise UpstreamWriteError("cannot connect to write upstream")
            if resp.status_code >= 400:
                raise UpstreamWriteError(f"write upstream returned {resp.status_code}")
            try:
                return resp.json()
            except ValueError:
                raise UpstreamWriteError("write upstream returned a non-JSON response")

        submission = await self._idem.submit(key, fp, execute)
        logger.info("endpoint=write_item branch=done replayed=%s", submission.replayed)
        result = dict(submission.result)
        result["idempotent_replay"] = submission.replayed
        result["idempotency_key"] = key
        return result


class UpstreamWriteError(Exception):
    """写上游失败（消息由网关构造，不含原始异常）。"""
