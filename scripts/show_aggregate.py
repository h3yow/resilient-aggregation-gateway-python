"""从 stdin 读取聚合响应 JSON 并格式化打印（供 demo.sh 使用）。"""

import json
import sys

d = json.load(sys.stdin)
print(f"  status={d['status']} reason={d['reason']}")
for r in d["results"]:
    print(f"    - {r['upstream']}: ok={r['ok']} rejected={r['rejected']} err={r['error']}")
