"""本地模拟上游：可编程延迟 / 错误 / 挂起，带调用计数。"""

from fastapi import FastAPI


def create_app(name: str = "mock") -> FastAPI:
    app = FastAPI(title=f"mock-upstream-{name}")
    app.state.upstream_name = name

    @app.get("/items")
    async def items():  # pragma: no cover - 骨架
        raise NotImplementedError

    @app.post("/items")
    async def create_item():  # pragma: no cover - 骨架
        raise NotImplementedError

    @app.get("/__stats__")
    async def stats():  # pragma: no cover - 骨架
        raise NotImplementedError

    @app.post("/__control__")
    async def control():  # pragma: no cover - 骨架
        raise NotImplementedError

    return app
