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

if [[ -z "${DINGTALK_STREAM_CLIENT_ID:-}" || -z "${DINGTALK_STREAM_CLIENT_SECRET:-}" ]]; then
  echo "[ERROR] 请先设置 DINGTALK_STREAM_CLIENT_ID / DINGTALK_STREAM_CLIENT_SECRET"
  echo "[HINT] export DINGTALK_STREAM_CLIENT_ID='你的ClientId'"
  echo "[HINT] export DINGTALK_STREAM_CLIENT_SECRET='你的ClientSecret'"
  exit 2
fi

if [[ -z "${DINGTALK_REPLY_WEBHOOK:-}" ]]; then
  echo "[WARN] DINGTALK_REPLY_WEBHOOK 未设置，将依赖消息中的 sessionWebhook 回包。"
fi

echo "[INFO] Starting DingTalk Stream bridge..."
exec "${VENV_DIR}/bin/python" "${ROOT}/dingtalk_stream_bridge.py"
