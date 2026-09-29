"""幂等键存储：同键去重、并发单飞、冲突与过期区分。"""

import enum
from typing import Any

from .config import Clock


class IdempotencyErrorCode(str, enum.Enum):
    KEY_MISSING = "key_missing"      # 未提供幂等键
    KEY_CONFLICT = "key_conflict"    # 同键但请求指纹不同
    KEY_EXPIRED = "key_expired"      # 键已过期被清除
    IN_PROGRESS = "in_progress"      # 同键请求正在执行（仅查询场景可见）


class IdempotencyError(Exception):
    def __init__(self, code: IdempotencyErrorCode, message: str):
        super().__init__(message)
        self.code = code


class IdempotencyStore:
    """内存幂等存储：同键并发只执行一次，重复提交返回首次结果。"""

    def __init__(self, ttl: float, clock: Clock, key_max_len: int = 128):
        self._ttl = ttl
        self._clock = clock
        self._key_max_len = key_max_len

    async def submit(self, key: str | None, fingerprint: str, execute) -> Any:  # pragma: no cover - 骨架
        raise NotImplementedError
