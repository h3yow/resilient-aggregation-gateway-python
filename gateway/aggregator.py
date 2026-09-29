"""聚合读：并发调用多个上游并分类汇总结果。

结果分类：
- success：所有上游都成功。
- partial_success：至少一个成功，其余失败或被熔断拒绝。
- all_failed：没有成功，且失败均来自上游自身（超时/报错）。
- rejected：没有成功，且至少一个调用在发出前就被熔断器快速拒绝
  （即故障是"被提前拒绝"而非已经尝试后失败）。
"""

import asyncio
import enum
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from .errors import CircuitOpenError

logger = logging.getLogger("gateway.aggregator")


class Upstream(Protocol):
    name: str

    async def get(self, path: str, params: dict[str, Any] | None = ...) -> dict[str, Any]: ...


class AggregateStatus(str, enum.Enum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    ALL_FAILED = "all_failed"
    REJECTED = "rejected"


@dataclass
class UpstreamResult:
    name: str
    ok: bool
    data: dict[str, Any] | None = None
    error: str | None = None
    rejected: bool = False


@dataclass
class AggregateResponse:
    status: AggregateStatus
    results: list[UpstreamResult] = field(default_factory=list)
    reason: str | None = None


class Aggregator:
    def __init__(self, clients: list[Upstream], *, path: str = "/items") -> None:
        self._clients = clients
        self._path = path

    async def _call_one(self, client: Upstream, key: str) -> UpstreamResult:
        try:
            data = await client.get(self._path, params={"key": key})
            logger.info("aggregate 输入 key=%s upstream=%s 命中分支=成功 判定依据=2xx",
                        key, client.name)
            return UpstreamResult(name=client.name, ok=True, data=data)
        except CircuitOpenError as exc:
            logger.info("aggregate 输入 key=%s upstream=%s 命中分支=熔断提前拒绝 判定依据=%s",
                        key, client.name, exc)
            return UpstreamResult(name=client.name, ok=False, error=str(exc), rejected=True)
        except Exception as exc:  # noqa: BLE001 - 网关边界：任何剩余异常都不得透出
            logger.info("aggregate 输入 key=%s upstream=%s 命中分支=调用失败 判定依据=%s",
                        key, client.name, exc)
            reason = str(exc) or f"上游[{client.name}]调用失败"
            return UpstreamResult(name=client.name, ok=False, error=reason)

    async def query_all(self, key: str) -> AggregateResponse:
        if not key or not key.strip():
            logger.info("aggregate 输入 key=%r 命中分支=参数拒绝 判定依据=空 key", key)
            return AggregateResponse(
                status=AggregateStatus.REJECTED,
                reason="查询 key 不能为空",
            )
        logger.info("aggregate 输入 key=%s 命中分支=并发调用 判定依据=%d 个上游",
                    key, len(self._clients))
        results = await asyncio.gather(
            *(self._call_one(c, key) for c in self._clients)
        )
        results = list(results)
        successes = [r for r in results if r.ok]
        rejected = [r for r in results if r.rejected]

        if len(successes) == len(results):
            status = AggregateStatus.SUCCESS
            reason = None
        elif successes:
            status = AggregateStatus.PARTIAL_SUCCESS
            reason = (f"{len(successes)}/{len(results)} 个上游成功，"
                      f"其余 {len(results) - len(successes)} 个失败"
                      f"（其中 {len(rejected)} 个被熔断提前拒绝）")
        elif rejected:
            status = AggregateStatus.REJECTED
            reason = (f"全部 {len(results)} 个上游均未返回数据，"
                      f"其中 {len(rejected)} 个被熔断器提前拒绝（未实际发起调用）")
        else:
            status = AggregateStatus.ALL_FAILED
            reason = f"全部 {len(results)} 个上游调用均失败（已尝试发起调用）"
        logger.info("aggregate key=%s 命中分支=汇总完成 判定依据=%s", key, reason or "全部成功")
        return AggregateResponse(status=status, results=results, reason=reason)
