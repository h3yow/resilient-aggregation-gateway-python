"""配置模型：上游列表、超时、熔断、幂等 TTL 等。"""

from dataclasses import dataclass, field
from typing import Callable

#: 单调时钟注入点，便于测试中模拟冷却时间
Clock = Callable[[], float]


@dataclass(frozen=True)
class UpstreamConfig:
    """单个上游的配置。"""

    name: str
    base_url: str
    timeout: float = 0.5
    failure_threshold: int = 3
    recovery_timeout: float = 1.0
    half_open_probes: int = 1
    success_threshold: int = 1


@dataclass(frozen=True)
class GatewayConfig:
    """聚合网关整体配置。"""

    upstreams: tuple[UpstreamConfig, ...]
    request_timeout: float = 2.0
    idempotency_ttl: float = 300.0
    idempotency_key_max_len: int = 128


def default_config() -> GatewayConfig:
    """骨架阶段的默认配置（运行时由环境变量 / 启动参数覆盖）。"""
    raise NotImplementedError
