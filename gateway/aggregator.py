"""并发聚合多个上游，分类结果并屏蔽原始异常。"""

import enum
from dataclasses import dataclass, field
from typing import Any

from .config import GatewayConfig


class ResultCategory(str, enum.Enum):
    """聚合结果分类。"""

    SUCCESS = "success"                    # 全部上游成功
    PARTIAL_SUCCESS = "partial_success"    # 部分上游成功
    ALL_FAILED = "all_failed"              # 所有上游都被调用且全部失败
    REJECTED = "rejected"                  # 全部上游在调用前被熔断拒绝


class FailureKind(str, enum.Enum):
    """单上游失败原因分类（可读、可枚举，不透出原始异常）。"""

    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    UPSTREAM_ERROR = "upstream_error"      # 上游返回 5xx
    BAD_RESPONSE = "bad_response"          # 响应体无法解析
    CIRCUIT_OPEN = "circuit_open"          # 被熔断器提前拒绝


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


class Aggregator:
    """并发调用所有上游并汇总。"""

    def __init__(self, config: GatewayConfig):
        self._config = config

    async def aggregate(self) -> AggregateResult:  # pragma: no cover - 骨架
        raise NotImplementedError
