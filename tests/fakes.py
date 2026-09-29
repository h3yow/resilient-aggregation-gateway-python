"""测试用的可控假上游。"""

import asyncio

from gateway.errors import CircuitOpenError, UpstreamCallError


class FakeUpstream:
    def __init__(self, name: str, *, delay: float = 0.0) -> None:
        self.name = name
        self.delay = delay
        self.calls = 0
        # mode: "ok" | dict 返回值 | Exception 实例 | "hang"
        self.mode: object = {"items": []}
        self._hang: dict[str, asyncio.Event] = {}

    async def get(self, path, params=None):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.mode == "hang":
            event = asyncio.Event()
            self._hang[str(params)] = event
            try:
                await event.wait()
            finally:
                self._hang.pop(str(params), None)
            return {"items": []}
        if isinstance(self.mode, Exception):
            raise self.mode
        if isinstance(self.mode, dict):
            return self.mode
        raise RuntimeError("bad fake mode")

    def release_hangs(self) -> None:
        for event in list(self._hang.values()):
            event.set()


def timeout_error(name: str) -> UpstreamCallError:
    return UpstreamCallError(f"上游[{name}]在 1.0s 内未响应（超时）")


def circuit_open_error(name: str) -> CircuitOpenError:
    return CircuitOpenError(f"熔断器[{name}]打开中，快速拒绝")
