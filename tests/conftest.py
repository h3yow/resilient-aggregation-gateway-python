"""测试夹具：本地真实启动 3 个模拟上游（uvicorn 线程），网关走 ASGI in-process。"""

import logging
import socket
import threading
import time

import httpx
import pytest
import pytest_asyncio
import uvicorn

from gateway.config import GatewayConfig, UpstreamConfig
from gateway.main import create_app
from gateway.service import GatewayService
from mock_upstream.app import create_app as create_mock_app

NAMES = ("alpha", "beta", "gamma")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningServer:
    """在独立线程里跑 uvicorn，端口预先分配好。"""

    def __init__(self, app, port: int):
        self.port = port
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self) -> None:
        self._thread.start()
        while not self._server.started:
            time.sleep(0.005)

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)


@pytest.fixture(scope="session")
def mock_servers():
    servers = {}
    try:
        for name in NAMES:
            server = RunningServer(create_mock_app(name), _free_port())
            server.start()
            servers[name] = server
        yield servers
    finally:
        for server in servers.values():
            server.stop()


@pytest.fixture
def upstream_urls(mock_servers):
    return {name: f"http://127.0.0.1:{s.port}" for name, s in mock_servers.items()}


@pytest_asyncio.fixture
async def env(upstream_urls):
    """新服务实例 + 用于控制上游 / 访问网关的两个 client。"""
    upstreams = tuple(
        UpstreamConfig(name=n, base_url=upstream_urls[n], timeout=0.4,
                       failure_threshold=3, recovery_timeout=0.8,
                       half_open_probes=1, success_threshold=1)
        for n in NAMES
    )
    config = GatewayConfig(upstreams=upstreams, idempotency_ttl=60.0)
    service = GatewayService(config)
    app = create_app(service)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gateway") as gw, \
            httpx.AsyncClient() as control:
        for url in upstream_urls.values():
            await control.post(f"{url}/__reset__")
        yield {"service": service, "gw": gw, "control": control, "urls": upstream_urls}
        await service.aclose()


def control(client: httpx.AsyncClient, base_url: str, **payload):
    return client.post(f"{base_url}/__control__", json=payload)


def stats(client: httpx.AsyncClient, base_url: str):
    return client.get(f"{base_url}/__stats__")


@pytest.fixture(autouse=True)
def _log_test_boundaries(request):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    logger = logging.getLogger("test")
    logger.info("==== TEST START %s ====", request.node.name)
    yield
    logger.info("==== TEST END   %s ====", request.node.name)
