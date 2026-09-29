"""上游 HTTP 客户端。

- 每次调用经过熔断器申请许可（熔断打开时快速失败）。
- 统一超时控制。
- 任何上游异常都转换为脱敏后的 UpstreamCallError，不向外泄露原始堆栈。
"""

import logging
from typing import Any

import httpx

from .circuit import CircuitBreaker
from .config import UpstreamConfig
from .errors import UpstreamCallError

logger = logging.getLogger("gateway.upstream")


class UpstreamClient:
    def __init__(
        self,
        config: UpstreamConfig,
        breaker: CircuitBreaker,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        self._breaker = breaker
        self._http = httpx.AsyncClient(
            base_url=config.base_url, timeout=config.timeout, transport=transport
        )

    @property
    def name(self) -> str:
        return self.config.name

    async def aclose(self) -> None:
        await self._http.aclose()

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        logger.info("upstream=%s 输入 path=%s params=%s", self.name, path, params)
        guard = await self._breaker.acquire()  # 熔断打开时抛 CircuitOpenError
        # 所有结果判定都必须在 guard 内，熔断器才能正确记录成功/失败
        async with guard:
            try:
                resp = await self._http.get(path, params=params)
            except httpx.TimeoutException:
                logger.info("upstream=%s 命中分支=超时 判定依据=%.2fs 内未返回",
                            self.name, self.config.timeout)
                raise UpstreamCallError(
                    f"上游[{self.name}]在 {self.config.timeout:.1f}s 内未响应（超时）"
                ) from None
            except httpx.TransportError:
                logger.info("upstream=%s 命中分支=连接失败 判定依据=网络/连接错误", self.name)
                raise UpstreamCallError(f"上游[{self.name}]暂时不可达（连接失败）") from None

            if resp.status_code >= 500:
                logger.info("upstream=%s 命中分支=上游错误 判定依据=HTTP %d",
                            self.name, resp.status_code)
                raise UpstreamCallError(
                    f"上游[{self.name}]返回服务端错误（HTTP {resp.status_code}）")
            if resp.status_code >= 400:
                logger.info("upstream=%s 命中分支=请求被拒 判定依据=HTTP %d",
                            self.name, resp.status_code)
                raise UpstreamCallError(
                    f"上游[{self.name}]拒绝了请求（HTTP {resp.status_code}）")
            try:
                payload = resp.json()
            except ValueError:
                logger.info("upstream=%s 命中分支=响应非法 判定依据=非 JSON 响应", self.name)
                raise UpstreamCallError(f"上游[{self.name}]返回了无法解析的响应") from None

        logger.info("upstream=%s 命中分支=成功 判定依据=HTTP 2xx JSON", self.name)
        return payload if isinstance(payload, dict) else {"data": payload}
