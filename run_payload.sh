#!/usr/bin/env bash
set -euo pipefail

# 可按需改这两个文件名
PAYLOAD_FILE="${1:-payload.json}"
OUT_FILE="${2:-tools_invoke_result.json}"

if [[ ! -f "$PAYLOAD_FILE" ]]; then
  echo "[ERROR] payload file not found: $PAYLOAD_FILE" >&2
  exit 1
fi

TOKEN="$(python3 - <<'PY'
import json, os, sys
p = os.path.expanduser("~/.openclaw/openclaw.json")
try:
    with open(p, "r", encoding="utf-8") as f:
        print(json.load(f)["gateway"]["auth"]["token"])
except Exception as e:
    print(f"[ERROR] failed to read token from {p}: {e}", file=sys.stderr)
    sys.exit(1)
PY
)"

if [[ -z "${TOKEN}" ]]; then
  echo "[ERROR] empty token" >&2
  exit 1
fi

echo "[INFO] invoking /tools/invoke with payload: ${PAYLOAD_FILE}"
HTTP_CODE="$(curl -sS -o "${OUT_FILE}" -w "%{http_code}" \
  "http://127.0.0.1:18789/tools/invoke" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d @"${PAYLOAD_FILE}")"

echo "[INFO] HTTP status: ${HTTP_CODE}"
echo "[INFO] response saved to: ${OUT_FILE}"

# 额外打印一份便于肉眼看
if command -v jq >/dev/null 2>&1; then
  echo "[INFO] pretty response:"
  jq . "${OUT_FILE}" || cat "${OUT_FILE}"
else
  cat "${OUT_FILE}"
fi

# 非 2xx 时返回失败码，方便 CI/脚本判断
if [[ ! "${HTTP_CODE}" =~ ^2 ]]; then
  exit 1
fi