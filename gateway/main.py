"""FastAPI 应用工厂与路由。"""

from fastapi import FastAPI

from .config import GatewayConfig
from .service import GatewayService


def create_app(service: GatewayService) -> FastAPI:
    app = FastAPI(title="resilient-aggregation-gateway")
    app.state.service = service

    @app.get("/aggregate/items")
    async def aggregate_items():  # pragma: no cover - 骨架
        raise NotImplementedError

    @app.post("/items")
    async def create_item():  # pragma: no cover - 骨架
        raise NotImplementedError

    return app
