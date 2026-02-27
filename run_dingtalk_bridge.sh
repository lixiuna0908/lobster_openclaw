#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/work/000code/github"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${ROOT}/.venv_dingtalk_bridge"

cd "${ROOT}"

if [[ ! -d "${VENV_DIR}" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

source "${VENV_DIR}/bin/activate"
pip install --upgrade pip >/dev/null
pip install -r "${ROOT}/requirements.txt"

# Ensure conda base tools (e.g. gatk) are resolvable by downstream subprocesses.
if [[ -d "$HOME/miniconda3/bin" ]]; then
  export PATH="$HOME/miniconda3/bin:$PATH"
fi

if [[ -z "${DINGTALK_SIGN_SECRET:-}" ]]; then
  echo "[WARN] DINGTALK_SIGN_SECRET 未设置，当前不校验签名。"
fi
if [[ -z "${DINGTALK_CALLBACK_TOKEN:-}" || -z "${DINGTALK_CALLBACK_AES_KEY:-}" ]]; then
  echo "[WARN] DINGTALK_CALLBACK_TOKEN / DINGTALK_CALLBACK_AES_KEY 未设置，钉钉事件订阅加密回调将无法通过。"
fi
if [[ -z "${DINGTALK_REPLY_WEBHOOK:-}" ]]; then
  echo "[WARN] DINGTALK_REPLY_WEBHOOK 未设置，异步回执将依赖回调体中的 sessionWebhook。"
fi

export DINGTALK_BRIDGE_HOST="${DINGTALK_BRIDGE_HOST:-0.0.0.0}"
export DINGTALK_BRIDGE_PORT="${DINGTALK_BRIDGE_PORT:-8788}"

echo "[INFO] Starting DingTalk bridge at ${DINGTALK_BRIDGE_HOST}:${DINGTALK_BRIDGE_PORT}"
exec "${VENV_DIR}/bin/uvicorn" dingtalk_bridge:app --host "${DINGTALK_BRIDGE_HOST}" --port "${DINGTALK_BRIDGE_PORT}"
