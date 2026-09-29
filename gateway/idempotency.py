"""幂等存储。

语义：
- begin(key, request_hash)：
  * key 为空 -> IdempotencyRejected(MISSING_KEY)
  * 首次出现 -> 返回 None，表示当前调用拿到唯一执行权（in-flight 占位）
  * 同键同请求、已有结果 -> 返回首次 StoredResponse（不重复执行）
  * 同键同请求、仍在执行 -> 阻塞等待首个结果
  * 同键不同请求 -> IdempotencyRejected(CONFLICT)
  * 记录已过期 -> IdempotencyRejected(EXPIRED) 并清除
- complete(key, response)：写入最终结果并唤醒所有等待者。
- abort(key)：执行失败，清除占位并唤醒等待者（允许后续重试）。
"""

import asyncio
import enum
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger("gateway.idempotency")


class RejectReason(str, enum.Enum):
    MISSING_KEY = "missing_key"
    CONFLICT = "conflict"
    EXPIRED = "expired"


class IdempotencyRejected(Exception):
    def __init__(self, reason: RejectReason) -> None:
        self.reason = reason
        super().__init__(reason.value)


@dataclass
class StoredResponse:
    status_code: int
    body: dict[str, Any]
    created_at: float


@dataclass
class _Entry:
    request_hash: str
    response: StoredResponse | None = None  # None 表示首个请求仍在执行
    event: asyncio.Event | None = None


def hash_request(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class IdempotencyStore:
    def __init__(self, ttl_seconds: float, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._ttl = ttl_seconds
        self._clock = clock
        self._entries: dict[str, _Entry] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, key: str) -> asyncio.Lock:
        # 调用方需已持全局短锁；惰性创建 key 级锁，保证同键并发串行进入判定
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def begin(self, key: str, request_hash: str) -> StoredResponse | None:
        if not key or not key.strip():
            logger.info("idempotency 输入 key=%r 命中分支=缺键 判定依据=幂等键为空", key)
            raise IdempotencyRejected(RejectReason.MISSING_KEY)

        lock = self._lock_for(key)
        async with lock:
            entry = self._entries.get(key)
            now = self._clock()

            if entry is not None and entry.response is not None:
                age = now - entry.response.created_at
                if age > self._ttl:
                    logger.info("idempotency 输入 key=%s 命中分支=过期 判定依据=age=%.2fs > ttl=%.2fs",
                                key, age, self._ttl)
                    self._entries.pop(key, None)
                    raise IdempotencyRejected(RejectReason.EXPIRED)
                if entry.request_hash != request_hash:
                    logger.info("idempotency 输入 key=%s 命中分支=冲突 判定依据=请求指纹不同",
                                key)
                    raise IdempotencyRejected(RejectReason.CONFLICT)
                logger.info("idempotency 输入 key=%s 命中分支=命中首次结果 判定依据=指纹相同 age=%.2fs",
                            key, age)
                return entry.response

            if entry is None:
                entry = _Entry(request_hash=request_hash, event=asyncio.Event())
                self._entries[key] = entry
                logger.info("idempotency 输入 key=%s 命中分支=首次提交获得执行权 判定依据=无记录",
                            key)
                return None

            # 已有同键请求在执行
            if entry.request_hash != request_hash:
                logger.info("idempotency 输入 key=%s 命中分支=冲突 判定依据=在途请求指纹不同",
                            key)
                raise IdempotencyRejected(RejectReason.CONFLICT)

        # 在 key 锁外等待，避免阻塞首个请求完成；循环应对“首个失败后新请求占位”的竞态
        while True:
            async with lock:
                entry = self._entries.get(key)
                if entry is not None and entry.response is not None:
                    age = self._clock() - entry.response.created_at
                    if age > self._ttl:
                        self._entries.pop(key, None)
                        raise IdempotencyRejected(RejectReason.EXPIRED)
                    logger.info("idempotency 输入 key=%s 命中分支=复用首次结果 判定依据=在途请求已完成",
                                key)
                    return entry.response
                if entry is None:
                    logger.info("idempotency 输入 key=%s 命中分支=首次执行失败 判定依据=占位已释放",
                                key)
                    raise IdempotencyRejected(RejectReason.MISSING_KEY)
                event = entry.event
                assert event is not None
            logger.info("idempotency 输入 key=%s 命中分支=等待在途首次请求 判定依据=并发重复提交",
                        key)
            await event.wait()

    async def complete(self, key: str, response: StoredResponse) -> None:
        async with self._lock_for(key):
            entry = self._entries.get(key)
            if entry is not None:
                entry.response = response
                event, entry.event = entry.event, None
        if event is not None:
            event.set()
        logger.info("idempotency key=%s 命中分支=写入首次结果 判定依据=执行完成 status=%d",
                    key, response.status_code)

    async def abort(self, key: str) -> None:
        async with self._lock_for(key):
            entry = self._entries.pop(key, None)
            event = entry.event if entry is not None else None
        logger.info("idempotency key=%s 命中分支=清除占位 判定依据=执行失败允许重试", key)
        if event is not None:
            event.set()
