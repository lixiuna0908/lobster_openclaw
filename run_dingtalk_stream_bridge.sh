#!/usr/bin/env bash
set -euo pipefail

# 钉钉桥接与生信流程统一使用 conda 环境 gatk（含 GATK/PyTorch/scorevariants 及 dingtalk-stream 等依赖）。
ROOT="/Users/work/000code/github"
CONDA_ENV="${DINGTALK_CONDA_ENV:-gatk}"

cd "${ROOT}"

# 确保 conda 可用；GFORTRAN 避免 gatk 环境 activate 时 gfortran 脚本报 unbound variable
if [[ -d "$HOME/miniconda3/bin" ]]; then
  export PATH="$HOME/miniconda3/bin:$PATH"
fi
# 避免 gatk 环境 activate.d 里 set -u 报 GFORTRAN unbound
export GFORTRAN="${GFORTRAN:-}"

# 确保 gatk 环境下已安装钉钉桥接依赖
conda run -n "${CONDA_ENV}" python3 -c 'import dingtalk_stream' 2>/dev/null || {
  echo "[INFO] 在 conda 环境 ${CONDA_ENV} 中安装 requirements.txt..."
  conda run -n "${CONDA_ENV}" pip install -q -r "${ROOT}/requirements.txt"
}

# 这里用硬编码替换成你当前机器人的凭证（刚才日志中暴露的或者钉钉后台最新的）
export DINGTALK_STREAM_CLIENT_ID="dingnidkqchjoxh6rr4j"
export DINGTALK_STREAM_CLIENT_SECRET="uTM3I164R1bhMmcqjvqwJOpoFkKT0pCxFVq8mkhJSJZjnD3IEkS0Hz_lFbEoiQ-f"

if [[ -z "${DINGTALK_STREAM_CLIENT_ID:-}" || -z "${DINGTALK_STREAM_CLIENT_SECRET:-}" ]]; then
  echo "[ERROR] 请先设置 DINGTALK_STREAM_CLIENT_ID / DINGTALK_STREAM_CLIENT_SECRET"
  echo "[HINT] export DINGTALK_STREAM_CLIENT_ID='你的ClientId'"
  echo "[HINT] export DINGTALK_STREAM_CLIENT_SECRET='你的ClientSecret'"
  exit 2
fi

if [[ -z "${DINGTALK_REPLY_WEBHOOK:-}" ]]; then
  echo "[WARN] DINGTALK_REPLY_WEBHOOK 未设置，将依赖消息中的 sessionWebhook 回包。"
fi

echo "[INFO] Starting DingTalk Stream bridge [conda env: ${CONDA_ENV}]..."
exec env GFORTRAN="${GFORTRAN}" conda run -n "${CONDA_ENV}" python3 "${ROOT}/dingtalk_stream_bridge.py"
