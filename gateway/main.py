"""网关本地启动入口：uv run python -m gateway.main"""

import os

import uvicorn

from .app import create_app


def main() -> None:
    app = create_app()
    uvicorn.run(
        app,
        host=os.getenv("GATEWAY_HOST", "127.0.0.1"),
        port=int(os.getenv("GATEWAY_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
