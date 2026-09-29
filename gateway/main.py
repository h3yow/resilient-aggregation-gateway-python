"""FastAPI 应用工厂、路由与错误语义映射。

HTTP 状态语义：
    200  聚合成功（含部分成功，看 body.category）
    400  幂等键缺失 / 非法
    409  幂等键冲突（同键不同体）
    410  幂等键已过期
    502  写上游失败（不重复执行、不透出原始异常）
    503  聚合读全部失败 / 全部被熔断提前拒绝（body.category 区分）
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .aggregator import ResultCategory
from .config import GatewayConfig, default_config
from .idempotency import IdempotencyError, IdempotencyErrorCode
from .service import GatewayService, UpstreamWriteError

logger = logging.getLogger("gateway.http")

_IDEM_STATUS = {
    IdempotencyErrorCode.KEY_MISSING: 400,
    IdempotencyErrorCode.KEY_INVALID: 400,
    IdempotencyErrorCode.KEY_CONFLICT: 409,
    IdempotencyErrorCode.KEY_EXPIRED: 410,
}


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": {"code": code, "message": message}})


def create_app(service: GatewayService) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("branch=startup upstreams=%d", len(service._config.upstreams))
        yield
        await service.aclose()

    app = FastAPI(title="resilient-aggregation-gateway", lifespan=lifespan)
    app.state.service = service

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        started = time.monotonic()
        response = await call_next(request)
        logger.info("http %s %s -> %s %.3fms", request.method, request.url.path,
                    response.status_code, (time.monotonic() - started) * 1000)
        return response

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "upstreams": {name: cb.snapshot() for name, cb in service.breakers.items()},
            "idempotency": service.idempotency.snapshot(),
        }

    @app.get("/aggregate/items")
    async def aggregate_items() -> JSONResponse:
        result = await service.read_items()
        body = {
            "category": result.category.value,
            "upstreams_total": result.upstreams_total,
            "upstreams_succeeded": result.upstreams_succeeded,
            "data": result.data,
            "failures": [
                {
                    "upstream": f.upstream,
                    "kind": f.kind.value,
                    "message": f.message,
                    "status_code": f.status_code,
                }
                for f in result.failures
            ],
        }
        status = 200 if result.ok else 503
        return JSONResponse(status_code=status, content=body)

    @app.post("/items")
    async def create_item(request: Request) -> Any:
        key = request.headers.get("Idempotency-Key")
        try:
            payload = await request.json()
        except Exception:
            return _error_response(400, "invalid_json", "request body must be valid JSON")
        if not isinstance(payload, dict):
            return _error_response(400, "invalid_payload", "request body must be a JSON object")
        try:
            result = await service.write_item(key, payload)
        except IdempotencyError as exc:
            logger.info("branch=write_rejected code=%s key=%s", exc.code.value, key)
            return _error_response(_IDEM_STATUS[exc.code], exc.code.value, str(exc))
        except UpstreamWriteError as exc:
            logger.info("branch=write_upstream_failure reason=%s", exc)
            return _error_response(502, "upstream_write_failed", str(exc))
        return JSONResponse(status_code=200, content=result)

    return app


def build_app(config: GatewayConfig | None = None) -> FastAPI:
    """按配置（默认读环境变量）构建可直接 uvicorn 运行的 app。"""
    config = config or default_config()
    service = GatewayService(config)
    return create_app(service)


app = build_app()
