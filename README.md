# Resilient Aggregation Gateway

面向内部服务的聚合网关：并发聚合多个上游的读接口、带熔断与恢复的上游调用、
支持幂等键的写接口。配套可编程的本地模拟上游，无需任何外部服务、真实凭据或付费组件，
即可在本地验证各种异常场景下的行为。

## 结构

```
gateway/
  config.py          配置模型与环境变量解析
  circuit_breaker.py 每上游熔断器（closed/open/half_open 三态）
  aggregator.py      并发聚合与结果分类
  idempotency.py     幂等键存储（单飞 / 冲突 / 过期墓碑）
  service.py         业务编排（读聚合 + 幂等写）
  main.py            FastAPI 路由与错误语义映射
mock_upstream/
  app.py             可编程模拟上游（延迟 / 错误 / 挂起 + 调用计数）
run_local.py         一键启动 3 个模拟上游 + 网关
tests/               并发、超时、熔断、幂等场景测试
```

## 本地跑起来

```bash
uv sync
uv run python run_local.py
```

端口：网关 `18000`，模拟上游 `18101/18102/18103`（全部绑定 127.0.0.1）。

### 手动验证

```bash
# 全部正常 -> category=success
curl -s http://127.0.0.1:18000/aggregate/items

# 让 beta 返回 503 -> category=partial_success，failures 里给出可读原因
curl -s -X POST http://127.0.0.1:18102/__control__ \
  -H 'content-type: application/json' -d '{"mode":"error","status_code":503}'
curl -s http://127.0.0.1:18000/aggregate/items

# 让 beta 变慢（超过上游超时）-> failure.kind=timeout
curl -s -X POST http://127.0.0.1:18102/__control__ \
  -H 'content-type: application/json' -d '{"mode":"sleep","delay":2.0}'

# 全部故障连续打满阈值 -> 先 all_failed，随后熔断 category=rejected（快速失败，不再打到上游）
for p in 18101 18102 18103; do
  curl -s -X POST http://127.0.0.1:$p/__control__ \
    -H 'content-type: application/json' -d '{"mode":"error"}' > /dev/null
done
for i in 1 2 3 4; do curl -s http://127.0.0.1:18000/aggregate/items; echo; done

# 查看各上游熔断器状态
curl -s http://127.0.0.1:18000/healthz

# 恢复上游，冷却结束后探测请求会把熔断器带回 closed
for p in 18101 18102 18103; do
  curl -s -X POST http://127.0.0.1:$p/__control__ \
    -H 'content-type: application/json' -d '{"mode":"ok"}' > /dev/null
done
sleep 1.2 && curl -s http://127.0.0.1:18000/aggregate/items

# 幂等写：同键同体第二次返回首次结果（idempotent_replay=true），上游只执行一次
curl -s -X POST http://127.0.0.1:18000/items -H 'content-type: application/json' \
  -H 'Idempotency-Key: demo-1' -d '{"name":"widget"}'
curl -s -X POST http://127.0.0.1:18000/items -H 'content-type: application/json' \
  -H 'Idempotency-Key: demo-1' -d '{"name":"widget"}'

# 模拟上游的调用计数（验证"只执行一次"）
curl -s http://127.0.0.1:18101/__stats__
```

### 跑测试

```bash
uv run pytest -q
```

19 个用例覆盖：聚合并发、单上游超时 / 5xx、部分成功、全部失败、熔断进入与快速失败、
冷却探测恢复、抖动不误触发、并发单探测、幂等缺失 / 冲突 / 过期 / 重复提交 / 并发单飞。
测试日志（`log_cli` 已开启）会打印每个用例的输入、命中的分支与判定依据。

## 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATEWAY_UPSTREAMS` | `alpha=http://127.0.0.1:18101,beta=…:18102,gamma=…:18103` | 逗号分隔的 `name=url`；只给 URL 时自动命名 |
| `GATEWAY_UPSTREAM_TIMEOUT` | `0.5` | 单上游请求超时（秒） |
| `GATEWAY_FAILURE_THRESHOLD` | `3` | 连续失败多少次触发熔断（成功会重置计数，短暂抖动不触发） |
| `GATEWAY_RECOVERY_TIMEOUT` | `1.0` | 熔断冷却时间（秒），冷却结束进入半开 |
| `GATEWAY_HALF_OPEN_PROBES` | `1` | 半开状态允许的最大并发探测数，超出快速失败 |
| `GATEWAY_SUCCESS_THRESHOLD` | `1` | 半开状态连续成功多少次恢复为 closed |
| `GATEWAY_IDEMPOTENCY_TTL` | `300` | 幂等键保留时间（秒），过期后同键拒绝（410） |

## 聚合读 `GET /aggregate/items`

并发调用全部上游，单个上游超时 / 报错不影响整体。响应体：

```json
{
  "category": "partial_success",
  "upstreams_total": 3,
  "upstreams_succeeded": 2,
  "data": {"alpha": {...}, "gamma": {...}},
  "failures": [{"upstream": "beta", "kind": "upstream_error",
                "message": "upstream returned server error 503", "status_code": 503}]
}
```

### 结果分类（category）

| category | 含义 | HTTP |
| --- | --- | --- |
| `success` | 全部上游成功 | 200 |
| `partial_success` | 部分上游成功，失败原因在 `failures` | 200 |
| `all_failed` | 所有上游都被真实调用且全部失败 | 503 |
| `rejected` | 所有上游都在调用前被熔断器提前拒绝（未打到上游） | 503 |

### 单上游失败原因（failures[].kind）

`timeout`（超时）、`connection_error`（连接失败）、`upstream_error`（上游返回 4xx/5xx）、
`bad_response`（响应非 JSON）、`circuit_open`（被熔断提前拒绝）。
`message` 由网关构造，可读且不包含上游原始异常。

## 熔断语义

- **closed**：正常放行；连续失败（5xx / 超时 / 连接失败，4xx 不计）达到阈值进入 open。
  中途任何一次成功都会重置计数，因此短暂抖动不会误触发。
- **open**：请求快速失败（`circuit_open`），不打到上游；冷却 `GATEWAY_RECOVERY_TIMEOUT` 秒后进入 half_open。
- **half_open**：最多放行 `GATEWAY_HALF_OPEN_PROBES` 个并发探测，其余请求仍快速失败
  （并发下不会重复探测）；探测连续成功 `GATEWAY_SUCCESS_THRESHOLD` 次回到 closed，
  任一探测失败立即回到 open 并重新计时。

## 幂等写 `POST /items`

请求头必须带 `Idempotency-Key`。同一个键 + 相同请求体：

- 重复提交返回**首次的结果**（`idempotent_replay: true`），不会重复执行；
- 并发提交只真正执行一次，其余等待并拿到同一结果；
- 执行失败不占用键，恢复后可用同键重试。

### 错误语义

| HTTP | error.code | 含义 |
| --- | --- | --- |
| 400 | `key_missing` / `key_invalid` | 未提供幂等键 / 键为空或超长 |
| 400 | `invalid_json` / `invalid_payload` | 请求体不是合法 JSON 对象 |
| 409 | `key_conflict` | 同一幂等键携带了不同的请求体 |
| 410 | `key_expired` | 幂等键已过 TTL，需换新键 |
| 502 | `upstream_write_failed` | 写上游失败（消息由网关构造，不透出原始异常） |

## 模拟上游控制面

每个模拟上游提供：

- `POST /__control__` `{"mode": "ok|sleep|error|hang", "delay": 秒, "status_code": 503}` — 注入行为
- `POST /__reset__` — 恢复默认并清零计数
- `GET /__stats__` — `get_items_calls` / `post_items_calls` / `max_concurrent_gets` 等计数
