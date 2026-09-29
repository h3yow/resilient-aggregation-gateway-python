"""本地模拟上游（无任何外部依赖）。

单进程同时启动多个 HTTP 服务，每个服务行为由全局模式决定：
- ok：正常返回 JSON
- slow：延迟超过超时
- error：返回 500
- crash：直接关闭连接（连接错误）

启动后可调用任意实例的 POST /__admin/mode/{name}?behavior=... 单独切换，
用于演示熔断恢复。

用法：
    uv run python scripts/mock_upstream.py            # 3 个实例 :8001-8003
    BEHAVIOR=slow uv run python scripts/mock_upstream.py
"""

import asyncio
import json
import logging
import os

import uvicorn
from fastapi import FastAPI, Query, Response

logger = logging.getLogger("mock-upstream")

# name -> behavior
MODES: dict[str, str] = {}


def make_app(name: str) -> FastAPI:
    app = FastAPI(title=f"mock-{name}")

    @app.get("/items")
    async def items(key: str, response: Response) -> Response:
        behavior = MODES.get(name, "ok")
        logger.info("mock name=%s 输入 GET /items key=%s 模式=%s", name, key, behavior)
        if behavior == "slow":
            await asyncio.sleep(5.0)
        if behavior == "error":
            return Response(
                content=json.dumps({"error": "simulated failure"}),
                status_code=500,
                media_type="application/json",
            )
        if behavior == "crash":
            # 非标准状态行模拟连接被对端重置
            return Response(content=b"", status_code=444)
        return Response(
            content=json.dumps({"upstream": name, "key": key, "payload": [1, 2, 3]}),
            media_type="application/json",
        )

    @app.post("/__admin/mode/{target}")
    async def set_mode(target: str, behavior: str = Query(...)) -> dict[str, str]:
        MODES[target] = behavior
        logger.info("mock admin 输入 target=%s behavior=%s", target, behavior)
        return {"target": target, "behavior": behavior}

    @app.get("/__admin/mode/{target}")
    async def get_mode(target: str) -> dict[str, str]:
        return {"target": target, "behavior": MODES.get(target, "ok")}

    return app


async def serve_all(names: list[str], base_port: int, default_behavior: str) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    for name in names:
        MODES[name] = default_behavior
    servers = []
    for i, name in enumerate(names):
        config = uvicorn.Config(make_app(name), host="127.0.0.1",
                                port=base_port + i, log_level="warning")
        server = uvicorn.Server(config)
        servers.append(server.serve())
    logger.info("mock upstreams=%s base_port=%d behavior=%s",
                names, base_port, default_behavior)
    await asyncio.gather(*servers)


def main() -> None:
    names = os.getenv("MOCK_NAMES", "svc-a,svc-b,svc-c").split(",")
    base_port = int(os.getenv("MOCK_BASE_PORT", "8001"))
    behavior = os.getenv("BEHAVIOR", "ok")
    asyncio.run(serve_all(names, base_port, behavior))


if __name__ == "__main__":
    main()
