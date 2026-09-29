"""配置模型：上游列表、超时、熔断、幂等 TTL，全部支持环境变量覆盖。

环境变量：
    GATEWAY_UPSTREAMS          逗号分隔的上游地址，形如 alpha=http://127.0.0.1:8101
                               （仅给 URL 时自动命名 upstream-1、upstream-2 ...）
    GATEWAY_UPSTREAM_TIMEOUT   单上游请求超时（秒，默认 0.5）
    GATEWAY_FAILURE_THRESHOLD  连续失败多少次触发熔断（默认 3）
    GATEWAY_RECOVERY_TIMEOUT   熔断冷却时间（秒，默认 1.0）
    GATEWAY_HALF_OPEN_PROBES   半开状态允许的并发探测数（默认 1）
    GATEWAY_SUCCESS_THRESHOLD  半开状态连续成功多少次恢复（默认 1）
    GATEWAY_IDEMPOTENCY_TTL    幂等键保留时间（秒，默认 300）
"""

import os
import time
from dataclasses import dataclass
from typing import Callable

#: 单调时钟注入点，便于测试中模拟冷却 / 过期
Clock = Callable[[], float]

DEFAULT_UPSTREAMS = "alpha=http://127.0.0.1:8101,beta=http://127.0.0.1:8102,gamma=http://127.0.0.1:8103"


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

    @property
    def write_upstream(self) -> UpstreamConfig:
        return self.upstreams[0]


def _parse_upstreams(spec: str) -> tuple[tuple[str, str], ...]:
    parsed = []
    for i, part in enumerate(spec.split(","), start=1):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            name, url = part.split("=", 1)
            name, url = name.strip(), url.strip()
        else:
            name, url = f"upstream-{i}", part
        parsed.append((name, url.rstrip("/")))
    return tuple(parsed)


def default_config() -> GatewayConfig:
    """从环境变量构建配置。"""
    timeout = float(os.getenv("GATEWAY_UPSTREAM_TIMEOUT", "0.5"))
    threshold = int(os.getenv("GATEWAY_FAILURE_THRESHOLD", "3"))
    recovery = float(os.getenv("GATEWAY_RECOVERY_TIMEOUT", "1.0"))
    probes = int(os.getenv("GATEWAY_HALF_OPEN_PROBES", "1"))
    success = int(os.getenv("GATEWAY_SUCCESS_THRESHOLD", "1"))
    idem_ttl = float(os.getenv("GATEWAY_IDEMPOTENCY_TTL", "300"))
    upstreams = tuple(
        UpstreamConfig(
            name=name,
            base_url=url,
            timeout=timeout,
            failure_threshold=threshold,
            recovery_timeout=recovery,
            half_open_probes=probes,
            success_threshold=success,
        )
        for name, url in _parse_upstreams(os.getenv("GATEWAY_UPSTREAMS", DEFAULT_UPSTREAMS))
    )
    return GatewayConfig(upstreams=upstreams, idempotency_ttl=idem_ttl)


def monotonic_clock() -> Clock:
    return time.monotonic
