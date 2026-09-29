"""本地一键启动：3 个模拟上游 + 1 个聚合网关，全部绑定 127.0.0.1。

用法：uv run python run_local.py
端口：上游 8101/8102/8103，网关 8080，Ctrl-C 退出。
"""

import asyncio
import logging

import uvicorn

from gateway.main import build_app
from mock_upstream.app import create_app

UPSTREAM_PORTS = {"alpha": 8101, "beta": 8102, "gamma": 8103}
GATEWAY_PORT = 8080


async def _start(server: uvicorn.Server) -> asyncio.Task:
    task = asyncio.create_task(server.serve())
    await server.startup()
    return task


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    tasks: list[asyncio.Task] = []
    for name, port in UPSTREAM_PORTS.items():
        config = uvicorn.Config(create_app(name), host="127.0.0.1", port=port,
                                log_level="warning")
        tasks.append(await _start(uvicorn.Server(config)))

    gateway = uvicorn.Server(uvicorn.Config(build_app(), host="127.0.0.1",
                                            port=GATEWAY_PORT, log_level="info"))
    tasks.append(await _start(gateway))

    print(f"\n聚合网关:  http://127.0.0.1:{GATEWAY_PORT}")
    print(f"健康检查:  http://127.0.0.1:{GATEWAY_PORT}/healthz")
    print(f"聚合读:    http://127.0.0.1:{GATEWAY_PORT}/aggregate/items")
    print(f"接口文档:  http://127.0.0.1:{GATEWAY_PORT}/docs")
    print("模拟上游故障示例:")
    print(f"  curl -s -X POST http://127.0.0.1:8102/__control__ -H 'content-type: application/json' -d '{{\"mode\":\"error\"}}'")
    print("Ctrl-C 退出\n")

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    asyncio.run(main())
