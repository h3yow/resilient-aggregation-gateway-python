"""FastAPI 应用与路由装配。"""

import logging
import os
import time
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .aggregator import AggregateStatus, Aggregator
from .circuit import CircuitBreaker
from .config import CircuitConfig, IdempotencyConfig, Settings, UpstreamConfig
from .errors import UpstreamCallError
from .idempotency import (
    IdempotencyRejected,
    IdempotencyStore,
    RejectReason,
    StoredResponse,
    hash_request,
)
from .logging_setup import configure_logging
from .upstream import UpstreamClient

logger = logging.getLogger("gateway.api")

# 写接口实际执行动作的签名：接收请求体，返回 (status_code, body)
WriteAction = Callable[[dict[str, Any]], Awaitable[tuple[int, dict[str, Any]]]]


class WriteRequest(BaseModel):
    item: str
    qty: int = 1


def load_settings() -> Settings:
    """从环境变量读取配置：
    GATEWAY_UPSTREAMS="name1=http://127.0.0.1:8001,name2=..."
    """
    failure_threshold = int(os.getenv("GATEWAY_FAILURE_THRESHOLD", "5"))
    cooldown = float(os.getenv("GATEWAY_COOLDOWN_SECONDS", "2"))
    timeout = float(os.getenv("GATEWAY_UPSTREAM_TIMEOUT", "1.0"))
    ttl = float(os.getenv("GATEWAY_IDEMPOTENCY_TTL", "300"))

    raw = os.getenv(
        "GATEWAY_UPSTREAMS",
        "svc-a=http://127.0.0.1:8001,svc-b=http://127.0.0.1:8002,svc-c=http://127.0.0.1:8003",
    )
    upstreams: list[UpstreamConfig] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, url = part.partition("=")
        upstreams.append(UpstreamConfig(name=name.strip(), base_url=url.strip(), timeout=timeout))

    return Settings(
        upstreams=tuple(upstreams),
        circuit=CircuitConfig(
            failure_threshold=failure_threshold,
            cooldown_seconds=cooldown,
        ),
        idempotency=IdempotencyConfig(ttl_seconds=ttl),
    )


def _default_write_action(payload: dict[str, Any]) -> Awaitable[tuple[int, dict[str, Any]]]:
    async def _run() -> tuple[int, dict[str, Any]]:
        order_id = "ord-" + str(abs(hash(payload.get("item", ""))) % 100000)
        return 201, {"order_id": order_id, "received": payload}

    return _run()


def create_app(
    settings: Settings | None = None,
    *,
    aggregator: Aggregator | None = None,
    idempotency: IdempotencyStore | None = None,
    write_action: WriteAction | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> FastAPI:
    configure_logging()
    settings = settings or load_settings()

    app = FastAPI(title="resilient-aggregation-gateway")

    clients: list[UpstreamClient] = []
    if aggregator is None:
        for uc in settings.upstreams:
            breaker = CircuitBreaker(uc.name, settings.circuit, clock=clock)
            clients.append(UpstreamClient(uc, breaker))
        aggregator = Aggregator(clients)
    if idempotency is None:
        idempotency = IdempotencyStore(settings.idempotency.ttl_seconds, clock=clock)
    action = write_action or _default_write_action

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/aggregate")
    async def aggregate(key: str) -> dict[str, Any]:
        logger.info("http 输入 GET /v1/aggregate key=%s", key)
        result = await aggregator.query_all(key)
        http_status = {
            AggregateStatus.SUCCESS: 200,
            AggregateStatus.PARTIAL_SUCCESS: 200,
            AggregateStatus.ALL_FAILED: 502,
            AggregateStatus.REJECTED: 503,
        }[result.status]
        return JSONResponse(
            status_code=http_status,
            content={
                "status": result.status.value,
                "reason": result.reason,
                "results": [
                    {
                        "upstream": r.name,
                        "ok": r.ok,
                        "data": r.data,
                        "error": r.error,
                        "rejected": r.rejected,
                    }
                    for r in result.results
                ],
            },
        )

    @app.post("/v1/orders")
    async def create_order(
        request: Request,
        idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        body = await request.json()
        logger.info("http 输入 POST /v1/orders idempotency_key=%s body=%s",
                    idempotency_key, body)
        if not idempotency_key or not idempotency_key.strip():
            logger.info("orders 命中分支=拒绝 判定依据=缺少 Idempotency-Key 头")
            raise HTTPException(
                status_code=400,
                detail={"code": "missing_key",
                        "message": "必须提供 Idempotency-Key 请求头"},
            )

        request_hash = hash_request(body)
        try:
            stored = await idempotency.begin(idempotency_key, request_hash)
        except IdempotencyRejected as exc:
            mapping = {
                RejectReason.MISSING_KEY: (400, "missing_key", "幂等键无效或首次执行失败，请重试"),
                RejectReason.CONFLICT: (409, "conflict",
                                        "同一幂等键对应的请求内容与首次提交不一致"),
                RejectReason.EXPIRED: (410, "expired", "幂等键的首次结果已过期，请使用新键重试"),
            }
            status, code, message = mapping[exc.reason]
            logger.info("orders 命中分支=拒绝 code=%s 判定依据=%s", code, exc.reason.value)
            raise HTTPException(
                status_code=status, detail={"code": code, "message": message}
            ) from None

        if stored is not None:
            logger.info("orders 命中分支=幂等返回首次结果 判定依据=重复提交 status=%d",
                        stored.status_code)
            return JSONResponse(
                status_code=stored.status_code,
                content={**stored.body, "idempotent_replay": True},
            )

        try:
            status_code, response_body = await action(body)
        except UpstreamCallError as exc:
            await idempotency.abort(idempotency_key)
            logger.info("orders 命中分支=执行失败不缓存 判定依据=%s", exc)
            raise HTTPException(status_code=502, detail={"code": "execution_failed",
                                                        "message": str(exc)}) from None
        except Exception:  # noqa: BLE001 - 边界兜底，不透出原始异常
            await idempotency.abort(idempotency_key)
            logger.exception("orders 命中分支=未知错误，已释放幂等占位")
            raise HTTPException(status_code=500,
                                detail={"code": "internal_error",
                                        "message": "订单处理失败，请使用同一幂等键重试"}
                                ) from None

        await idempotency.complete(
            idempotency_key,
            StoredResponse(status_code=status_code, body=response_body,
                           created_at=clock()),
        )
        logger.info("orders 命中分支=首次执行成功 判定依据=status=%d", status_code)
        return JSONResponse(
            status_code=status_code,
            content={**response_body, "idempotent_replay": False},
        )

    return app
