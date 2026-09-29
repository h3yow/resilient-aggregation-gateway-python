"""本地一键启动：3 个模拟上游 + 1 个聚合网关，全部绑定 127.0.0.1。

用法：uv run python run_local.py
端口：上游 18101/18102/18103，网关 18000，Ctrl-C 退出。
"""

import logging
import threading
import time

import uvicorn

from gateway.main import build_app
from mock_upstream.app import create_app

UPSTREAM_PORTS = {"alpha": 18101, "beta": 18102, "gamma": 18103}
GATEWAY_PORT = 18000


def _serve_in_thread(app, port: int) -> uvicorn.Server:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.01)
    return server


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    for name, port in UPSTREAM_PORTS.items():
        _serve_in_thread(create_app(name), port)
        print(f"模拟上游 {name}: http://127.0.0.1:{port}")

    print(f"\n聚合网关:  http://127.0.0.1:{GATEWAY_PORT}")
    print(f"健康检查:  http://127.0.0.1:{GATEWAY_PORT}/healthz")
    print(f"聚合读:    http://127.0.0.1:{GATEWAY_PORT}/aggregate/items")
    print(f"接口文档:  http://127.0.0.1:{GATEWAY_PORT}/docs")
    print("模拟上游故障示例:")
    print("  curl -s -X POST http://127.0.0.1:18102/__control__ "
          "-H 'content-type: application/json' -d '{\"mode\":\"error\"}'")
    print("Ctrl-C 退出\n")

    uvicorn.run(build_app(), host="127.0.0.1", port=GATEWAY_PORT, log_level="info")


if __name__ == "__main__":
    main()
