#!/usr/bin/env bash
# 本地端到端演示：启动模拟上游 + 网关，逐步制造故障并观察结果分类与熔断。
set -euo pipefail
cd "$(dirname "$0")/.."

BASE=http://127.0.0.1:8000
ADMIN=http://127.0.0.1:8001

echo ">> 启动模拟上游 (:8001-8003) 与网关 (:8000)"
uv run python scripts/mock_upstream.py > .mock.log 2>&1 &
MOCK_PID=$!
GATEWAY_FAILURE_THRESHOLD=3 GATEWAY_COOLDOWN_SECONDS=3 \
  uv run python -m gateway.main > .gateway.log 2>&1 &
GW_PID=$!
trap 'kill $MOCK_PID $GW_PID 2>/dev/null || true' EXIT

sleep 3
q() { curl -s "$BASE/v1/aggregate?key=$1" | uv run python scripts/show_aggregate.py; }

echo ">> 1) 全部正常"; q k
echo ">> 2) svc-c 返回 500：前两次 partial_success，连续3次后熔断"
curl -s -X POST "$ADMIN/__admin/mode/svc-c?behavior=error" >/dev/null
for i in 1 2 3 4; do echo "  第 $i 次"; q k; done
echo ">> 3) svc-c 恢复，冷却结束后探测成功回到 success"
curl -s -X POST "$ADMIN/__admin/mode/svc-c?behavior=ok" >/dev/null
sleep 3.5
q k
echo ">> 4) 写接口幂等（重复提交返回同一 order_id）"
curl -s -X POST "$BASE/v1/orders" -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" -d '{"item":"book","qty":1}'; echo
curl -s -X POST "$BASE/v1/orders" -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" -d '{"item":"book","qty":1}'; echo
echo ">> 日志见 .gateway.log / .mock.log"
