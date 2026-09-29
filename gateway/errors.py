"""网关对外暴露的错误类型（不包含上游原始异常）。"""


class GatewayError(Exception):
    """网关错误基类。"""


class UpstreamCallError(GatewayError):
    """上游调用失败（超时/连接错误/非 2xx），携带脱敏后的可读原因。"""


class CircuitOpenError(GatewayError):
    """熔断打开期间被快速拒绝。"""
