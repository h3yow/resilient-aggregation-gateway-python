"""配置项定义。"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class UpstreamConfig:
    name: str
    base_url: str
    timeout: float = 1.0


@dataclass(frozen=True)
class CircuitConfig:
    failure_threshold: int = 5
    cooldown_seconds: float = 2.0
    half_open_max_probes: int = 1


@dataclass(frozen=True)
class IdempotencyConfig:
    ttl_seconds: float = 300.0


@dataclass(frozen=True)
class Settings:
    upstreams: tuple[UpstreamConfig, ...] = field(default_factory=tuple)
    circuit: CircuitConfig = field(default_factory=CircuitConfig)
    idempotency: IdempotencyConfig = field(default_factory=IdempotencyConfig)
