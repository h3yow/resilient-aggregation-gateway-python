"""幂等键存储：同键去重、并发单飞、冲突与过期区分。

语义：
- 缺失 / 非法键      -> KEY_MISSING / KEY_INVALID，明确拒绝
- 同键同指纹         -> 并发只执行一次，重复提交返回首次结果（replayed=True）
- 同键不同指纹       -> KEY_CONFLICT，明确拒绝
- 键已过期（保留墓碑）-> KEY_EXPIRED，明确拒绝，不会静默重放或重执行
"""

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .config import Clock

logger = logging.getLogger("gateway.idempotency")


class IdempotencyErrorCode(str, enum.Enum):
    KEY_MISSING = "key_missing"
    KEY_INVALID = "key_invalid"
    KEY_CONFLICT = "key_conflict"
    KEY_EXPIRED = "key_expired"


class IdempotencyError(Exception):
    def __init__(self, code: IdempotencyErrorCode, message: str):
        super().__init__(message)
        self.code = code


@dataclass
class Submission:
    result: Any
    replayed: bool


@dataclass
class _Record:
    fingerprint: str
    expires_at: float
    event: asyncio.Event = field(default_factory=asyncio.Event)
    done: bool = False
    result: Any = None
    error: BaseException | None = None


class IdempotencyStore:
    """内存幂等存储（单进程语义，锁保护并发）。"""

    def __init__(self, ttl: float, clock: Clock = time.monotonic, key_max_len: int = 128):
        self._ttl = ttl
        self._clock = clock
        self._key_max_len = key_max_len
        self._records: dict[str, _Record] = {}
        self._tombstones: dict[str, float] = {}  # key -> 墓碑过期时刻
        self._lock = asyncio.Lock()

    async def submit(
        self,
        key: str | None,
        fingerprint: str,
        execute: Callable[[], Awaitable[Any]],
    ) -> Submission:
        if key is None or key == "":
            logger.info("branch=reject reason=key_missing")
            raise IdempotencyError(IdempotencyErrorCode.KEY_MISSING,
                                   "Idempotency-Key header is required")
        if len(key) > self._key_max_len or not key.strip():
            logger.info("branch=reject reason=key_invalid key_len=%d", len(key))
            raise IdempotencyError(IdempotencyErrorCode.KEY_INVALID,
                                   "Idempotency-Key is blank or too long")

        async with self._lock:
            self._purge_expired()
            if key in self._tombstones:
                logger.info("branch=reject reason=key_expired key=%s", key)
                raise IdempotencyError(IdempotencyErrorCode.KEY_EXPIRED,
                                       "idempotency key has expired; retry with a new key")
            rec = self._records.get(key)
            if rec is not None and rec.fingerprint != fingerprint:
                logger.info("branch=reject reason=key_conflict key=%s", key)
                raise IdempotencyError(IdempotencyErrorCode.KEY_CONFLICT,
                                       "idempotency key was used with a different payload")
            if rec is None:
                rec = _Record(fingerprint=fingerprint, expires_at=self._clock() + self._ttl)
                self._records[key] = rec
                creator = True
            else:
                creator = False

        if creator:
            logger.info("branch=execute key=%s", key)
            try:
                result = await execute()
            except BaseException as exc:
                rec.error = exc
                rec.event.set()
                async with self._lock:
                    self._records.pop(key, None)  # 执行失败不占用键，允许重试
                raise
            rec.result = result
            rec.done = True
            rec.event.set()
            return Submission(result=result, replayed=False)

        logger.info("branch=wait_in_flight key=%s", key)
        await rec.event.wait()
        if rec.error is not None:
            raise rec.error
        logger.info("branch=replay key=%s", key)
        return Submission(result=rec.result, replayed=True)

    def _purge_expired(self) -> None:
        """惰性清理：完成的过期记录转为墓碑，墓碑再保留一个 TTL 周期。"""
        now = self._clock()
        for key in [k for k, r in self._records.items() if r.done and now > r.expires_at]:
            del self._records[key]
            self._tombstones[key] = now + self._ttl
            logger.info("branch=expire key=%s", key)
        for key in [k for k, exp in self._tombstones.items() if now > exp]:
            del self._tombstones[key]

    def snapshot(self) -> dict[str, Any]:
        return {
            "keys": len(self._records),
            "tombstones": len(self._tombstones),
        }
