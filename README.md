# 内部服务聚合网关（Python / FastAPI）

一层面向内部服务的聚合网关，提供**并发聚合读**（带熔断与恢复）和**幂等写**。
全部异常场景都可以用仓库内置的本地模拟上游复现，不依赖任何外部服务、真实凭据或付费组件。

## 能力概览

- `GET /v1/aggregate?key=...`：并发调用全部上游并汇总；单个上游超时/报错不影响其余结果。
- 每个上游独立熔断器：连续失败达阈值才打开 → 冷却后半开探测 → 探测成功恢复、失败重新打开。
- `POST /v1/orders`：基于 `Idempotency-Key` 请求头做幂等，同键并发只真正执行一次。
- 上游的原始异常不会透出，网关只返回脱敏后的可读原因。
- 关键判定都会打印日志：**输入、命中分支、判定依据**。

## 目录结构

```
gateway/
  config.py           配置项（上游、熔断、幂等）
  errors.py           网关错误类型（脱敏后的错误）
  circuit.py          熔断器（闭合/打开/半开 + 探测名额 + 锁）
  upstream.py         上游 HTTP 客户端（超时、熔断接入、异常脱敏）
  aggregator.py       聚合读与结果分类
  idempotency.py      幂等存储（去重/冲突/过期/并发去重）
  app.py              FastAPI 装配、HTTP 路由、状态码映射
  main.py             启动入口
scripts/
  mock_upstream.py    本地模拟上游（单进程多端口，行为可动态切换）
  demo.sh             一键端到端演示
tests/                单元 + HTTP 层 + httpx 传输层测试
```

## 本地启动

依赖由 [uv](https://docs.astral.sh/uv/) 管理（需要 Python 3.14）。

```bash
# 终端 1：启动 3 个模拟上游（127.0.0.1:8001-8003）
uv run python scripts/mock_upstream.py

# 终端 2：启动网关（127.0.0.1:8000）
uv run python -m gateway.main

# 或一键演示（自动制造故障、展示熔断与幂等）
./scripts/demo.sh
```

模拟上游支持四种行为，可启动时用 `BEHAVIOR=ok|slow|error|crash` 指定默认值，
运行中再通过管理接口单独切换：

```bash
curl -X POST "http://127.0.0.1:8001/__admin/mode/svc-c?behavior=error"  # 500
curl -X POST "http://127.0.0.1:8001/__admin/mode/svc-c?behavior=slow"   # 超时
curl -X POST "http://127.0.0.1:8001/__admin/mode/svc-c?behavior=crash"  # 连接错误
curl -X POST "http://127.0.0.1:8001/__admin/mode/svc-c?behavior=ok"     # 恢复
```

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GATEWAY_HOST` / `GATEWAY_PORT` | `127.0.0.1` / `8000` | 网关监听地址 |
| `GATEWAY_UPSTREAMS` | `svc-a=http://127.0.0.1:8001,svc-b=...:8002,svc-c=...:8003` | 上游列表，`名称=base_url`，逗号分隔 |
| `GATEWAY_UPSTREAM_TIMEOUT` | `1.0`（秒） | 单次上游调用超时 |
| `GATEWAY_FAILURE_THRESHOLD` | `5` | 连续失败多少次后熔断打开（成功即清零，抖动不会误触发） |
| `GATEWAY_COOLDOWN_SECONDS` | `2` | 熔断打开后的冷却时间；到期才允许探测 |
| `GATEWAY_IDEMPOTENCY_TTL` | `300`（秒） | 幂等首次结果的保留时长 |

模拟上游另有 `MOCK_NAMES`、`MOCK_BASE_PORT`（默认 `svc-a,svc-b,svc-c` / `8001`）。

## 聚合读：结果分类与 HTTP 语义

响应体：

```json
{
  "status": "partial_success",
  "reason": "2/3 个上游成功，其余 1 个失败（其中 0 个被熔断提前拒绝）",
  "results": [
    {"upstream": "svc-a", "ok": true,  "data": {...}, "error": null, "rejected": false},
    {"upstream": "svc-c", "ok": false, "data": null,  "error": "上游[svc-c]在 1.0s 内未响应（超时）", "rejected": false}
  ]
}
```

| `status` | HTTP | 含义 |
| --- | --- | --- |
| `success` | 200 | 所有上游都成功 |
| `partial_success` | 200 | 至少一个成功；其余失败（超时/错误）或被熔断提前拒绝 |
| `all_failed` | 502 | 全部失败，且请求都**已经实际尝试**（超时、5xx、连接失败等） |
| `rejected` | 503 | 全部没拿到数据，且至少一个在发起调用前就被熔断器**提前拒绝** |

> “全部失败”和“被提前拒绝”因此被严格区分：前者是试过了不行，后者是熔断期内根本没放出去。
> 空 `key` 也返回 `rejected`（不会触达任何上游）。

每个失败项的 `error` 是网关注释过的中文可读原因，不含上游堆栈、内部 URL 之外的原始异常信息；
`rejected=true` 表示该结果来自熔断器快速拒绝。

## 熔断器语义

```
CLOSED（正常） --连续失败达阈值--> OPEN（快速拒绝）
OPEN   --冷却到期--> HALF_OPEN（只放 half_open_max_probes 个探测）
HALF_OPEN --探测成功且在途探测归零--> CLOSED
HALF_OPEN --任一探测失败--> OPEN（冷却时间重置）
```

- 成功一次就把连续失败计数清零，短暂抖动不会触发熔断。
- 半开期间探测名额受锁保护：并发请求不会出现重复探测，也不会在熔断期间被提前放行。
- 超时、连接错误、HTTP 5xx/4xx、非法响应都计为失败。

## 写接口幂等语义

`POST /v1/orders`，通过请求头 `Idempotency-Key` 标识，请求体的规范化 JSON 哈希作为“请求指纹”。

| 情况 | HTTP | 响应 `detail.code` |
| --- | --- | --- |
| 未提供幂等键 | 400 | `missing_key` |
| 同键 + 同请求体重复提交 | 返回**首次**结果（2xx 同首次状态码），`idempotent_replay=true`，不重复执行 |
| 同键 + 不同请求体（冲突） | 409 | `conflict` |
| 幂等记录已过期 | 410 | `expired` |
| 首次执行失败（不缓存结果） | 502 `execution_failed` / 500 `internal_error` | 用同一键重试可重新执行 |

并发行为：同一幂等键的并发请求中只有一个真正执行业务动作，其余请求等待首个结果并复用，
因此业务动作严格只发生一次。首次执行失败时会释放占位，允许用同一键安全重试。

## 测试

```bash
uv run pytest                      # 全部测试（带日志输出）
uv run pytest tests/test_circuit.py
uv run pytest tests/test_idempotency.py
uv run pytest tests/test_api.py    # HTTP 层：状态码/分类/幂等等
```

覆盖：并发聚合、单上游超时与报错隔离、熔断进入/冷却/探测恢复/失败重开、并发下不重复探测、
幂等重复提交与并发只执行一次、缺键/冲突/过期区分。日志使用 `命中分支=... 判定依据=...`
格式记录输入与判定过程，`pytest` 默认开启 INFO 级日志回显。
