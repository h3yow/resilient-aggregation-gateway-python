"""并发聚合多个上游，分类结果并屏蔽原始异常。

分类规则：
- 全部成功            -> SUCCESS
- 部分成功            -> PARTIAL_SUCCESS
- 全部失败（含真实调用）-> ALL_FAILED
- 全部被熔断提前拒绝    -> REJECTED
"""

import asyncio
import enum
import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

from .circuit_breaker import CircuitBreaker, CircuitOpenError
from .config import GatewayConfig, UpstreamConfig

logger = logging.getLogger("gateway.aggregator")


class ResultCategory(str, enum.Enum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    ALL_FAILED = "all_failed"
    REJECTED = "rejected"


class FailureKind(str, enum.Enum):
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    UPSTREAM_ERROR = "upstream_error"
    BAD_RESPONSE = "bad_response"
    CIRCUIT_OPEN = "circuit_open"


@dataclass(frozen=True)
class UpstreamFailure:
    upstream: str
    kind: FailureKind
    message: str
    status_code: int | None = None


@dataclass
class AggregateResult:
    category: ResultCategory
    data: dict[str, Any] = field(default_factory=dict)
    failures: list[UpstreamFailure] = field(default_factory=list)
    upstreams_total: int = 0
    upstreams_succeeded: int = 0

    @property
    def ok(self) -> bool:
        return self.category in (ResultCategory.SUCCESS, ResultCategory.PARTIAL_SUCCESS)


class _UpstreamServerError(Exception):
    """上游返回 5xx，计入熔断失败。"""

    def __init__(self, status_code: int):
        super().__init__(f"upstream returned {status_code}")
        self.status_code = status_code


class Aggregator:
    """并发调用所有上游并汇总；任何单上游故障都不外抛。"""

    def __init__(
        self,
        config: GatewayConfig,
        breakers: dict[str, CircuitBreaker],
        client: httpx.AsyncClient | None = None,
    ):
        self._config = config
        self._breakers = breakers
        self._client = client or httpx.AsyncClient()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def aggregate(self) -> AggregateResult:
        pairs = await asyncio.gather(
            *(self._fetch(up) for up in self._config.upstreams)
        )
        data: dict[str, Any] = {}
        failures: list[UpstreamFailure] = []
        for name, payload, failure in pairs:
            if failure is None:
                data[name] = payload
            else:
                failures.append(failure)

        total = len(self._config.upstreams)
        succeeded = len(data)
        if not failures:
            category = ResultCategory.SUCCESS
        elif succeeded:
            category = ResultCategory.PARTIAL_SUCCESS
        elif all(f.kind == FailureKind.CIRCUIT_OPEN for f in failures):
            category = ResultCategory.REJECTED
        else:
            category = ResultCategory.ALL_FAILED
        logger.info(
            "branch=aggregate_decision category=%s succeeded=%d/%d failure_kinds=%s",
            category.value, succeeded, total, [f.kind.value for f in failures],
        )
        return AggregateResult(
            category=category,
            data=data,
            failures=failures,
            upstreams_total=total,
            upstreams_succeeded=succeeded,
        )

    async def _fetch(self, up: UpstreamConfig):
        """返回 (name, payload, None) 或 (name, None, UpstreamFailure)。"""
        breaker = self._breakers[up.name]

        async def op() -> httpx.Response:
            resp = await self._client.get(f"{up.base_url}/items", timeout=up.timeout)
            if resp.status_code >= 500:
                raise _UpstreamServerError(resp.status_code)
            return resp

        try:
            resp = await breaker.call(op)
        except CircuitOpenError as exc:
            return self._fail(up, FailureKind.CIRCUIT_OPEN,
                              f"circuit breaker is open, retry after {exc.retry_after:.3f}s")
        except httpx.TimeoutException:
            return self._fail(up, FailureKind.TIMEOUT,
                              f"upstream did not respond within {up.timeout:.3f}s")
        except httpx.TransportError:
            return self._fail(up, FailureKind.CONNECTION_ERROR,
                              "failed to connect to upstream")
        except _UpstreamServerError as exc:
            return self._fail(up, FailureKind.UPSTREAM_ERROR,
                              f"upstream returned server error {exc.status_code}",
                              status_code=exc.status_code)
        except Exception:
            logger.exception("upstream=%s branch=unexpected_error", up.name)
            return self._fail(up, FailureKind.UPSTREAM_ERROR,
                              "unexpected error while calling upstream")

        if resp.status_code >= 400:
            # 4xx 是上游对请求本身的拒绝，不计入熔断失败
            return self._fail(up, FailureKind.UPSTREAM_ERROR,
                              f"upstream rejected the request with status {resp.status_code}",
                              status_code=resp.status_code)
        try:
            payload = resp.json()
        except ValueError:
            return self._fail(up, FailureKind.BAD_RESPONSE,
                              "upstream returned a non-JSON response")
        logger.info("upstream=%s branch=success", up.name)
        return up.name, payload, None

    @staticmethod
    def _fail(up: UpstreamConfig, kind: FailureKind, message: str,
              status_code: int | None = None):
        logger.info("upstream=%s branch=failure kind=%s reason=%s", up.name, kind.value, message)
        return up.name, None, UpstreamFailure(
            upstream=up.name, kind=kind, message=message, status_code=status_code,
        )
