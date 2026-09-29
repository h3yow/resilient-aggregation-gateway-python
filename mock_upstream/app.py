"""本地模拟上游：可编程的延迟 / 错误 / 挂起，带调用计数。

控制方式（测试与本地演示用，全部 127.0.0.1，无任何外部依赖）：
- POST /__control__ {"mode": "ok|sleep|error|hang", "delay": 秒, "status_code": 503}
- POST /__reset__   恢复默认并清零计数
- GET  /__stats__   查看调用计数
"""

import asyncio
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logger = logging.getLogger("mock_upstream")

# hang 模式下的服务端等待时间（客户端会更早超时，仅需保证服务端不提前返回）
_HANG_SECONDS = 3600.0


class ControlRequest(BaseModel):
    mode: str | None = None
    delay: float | None = None
    status_code: int | None = None


def create_app(name: str = "mock") -> FastAPI:
    app = FastAPI(title=f"mock-upstream-{name}")

    state = {
        "name": name,
        "mode": "ok",
        "delay": 0.2,
        "status_code": 500,
        "get_items_calls": 0,
        "post_items_calls": 0,
        "concurrent_gets": 0,
        "max_concurrent_gets": 0,
    }
    app.state.mock = state

    @app.get("/items")
    async def items():
        state["get_items_calls"] += 1
        state["concurrent_gets"] += 1
        state["max_concurrent_gets"] = max(state["max_concurrent_gets"], state["concurrent_gets"])
        logger.info("mock=%s endpoint=GET /items call=%d mode=%s",
                    name, state["get_items_calls"], state["mode"])
        try:
            if state["mode"] == "hang":
                await asyncio.sleep(_HANG_SECONDS)
            if state["mode"] == "sleep":
                await asyncio.sleep(state["delay"])
            if state["mode"] == "error":
                return JSONResponse(status_code=state["status_code"],
                                    content={"error": f"simulated {state['status_code']}"})
            return {"upstream": name, "items": [
                {"id": f"{name}-1", "name": f"item-from-{name}", "price": 100},
            ]}
        finally:
            state["concurrent_gets"] -= 1

    @app.post("/items")
    async def create_item(payload: dict):
        state["post_items_calls"] += 1
        logger.info("mock=%s endpoint=POST /items call=%d mode=%s payload_keys=%s",
                    name, state["post_items_calls"], state["mode"], list(payload))
        if state["mode"] == "sleep":
            await asyncio.sleep(state["delay"])
        if state["mode"] == "error":
            return JSONResponse(status_code=state["status_code"],
                                content={"error": f"simulated {state['status_code']}"})
        return {"upstream": name, "stored": payload, "seq": state["post_items_calls"]}

    @app.get("/__stats__")
    async def stats():
        return {
            "get_items_calls": state["get_items_calls"],
            "post_items_calls": state["post_items_calls"],
            "max_concurrent_gets": state["max_concurrent_gets"],
            "mode": state["mode"],
        }

    @app.post("/__control__")
    async def control(req: ControlRequest):
        if req.mode is not None:
            if req.mode not in ("ok", "sleep", "error", "hang"):
                return JSONResponse(status_code=400, content={"error": "unknown mode"})
            state["mode"] = req.mode
        if req.delay is not None:
            state["delay"] = req.delay
        if req.status_code is not None:
            state["status_code"] = req.status_code
        logger.info("mock=%s control=%s", name,
                    {k: state[k] for k in ("mode", "delay", "status_code")})
        return {k: state[k] for k in ("mode", "delay", "status_code")}

    @app.post("/__reset__")
    async def reset():
        state["mode"] = "ok"
        state["delay"] = 0.2
        state["status_code"] = 500
        state["get_items_calls"] = 0
        state["post_items_calls"] = 0
        state["concurrent_gets"] = 0
        state["max_concurrent_gets"] = 0
        return {"reset": True}

    return app
