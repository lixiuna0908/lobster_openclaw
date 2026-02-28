#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/work/000code/github"
PAYLOAD_FILE="${1:-bio_payload.json}"
OUT_FILE="${2:-bio_tools_invoke_result.json}"
FASTQ_PATH="${FASTQ_PATH:-${ROOT_DIR}/test_data/sample1.fastq}"
REF_PATH="${REF_PATH:-${ROOT_DIR}/refer_hg/hg38/hg38.fa.gz}"
OUTDIR_PATH="${OUTDIR_PATH:-${ROOT_DIR}/test_data/out}"
KNOWN_SITES_PATH="${KNOWN_SITES_PATH:-${ROOT_DIR}/dbsnp/dbsnp_hg19.vcf.gz}"
RUN_BQSR="${RUN_BQSR:-0}"
USE_LLM_TASK="${USE_LLM_TASK:-0}"
# Lobster tool timeout for long steps like first-time hg38 bwa index.
BIO_TOOL_TIMEOUT_MS="${BIO_TOOL_TIMEOUT_MS:-7200000}"

# Ensure conda base tools (e.g. gatk) are resolvable.
if [[ -d "$HOME/miniconda3/bin" ]]; then
  export PATH="$HOME/miniconda3/bin:$PATH"
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

python3 - <<'PY' "$PAYLOAD_FILE" "$TOKEN" "$FASTQ_PATH" "$REF_PATH" "$OUTDIR_PATH" "$KNOWN_SITES_PATH" "$RUN_BQSR" "$USE_LLM_TASK" "$BIO_TOOL_TIMEOUT_MS"
import json
import sys

payload_file, token, fastq_path, ref_path, outdir_path, known_sites_path, run_bqsr_str, use_llm_task_str, timeout_ms_str = sys.argv[1:10]
use_llm_task = use_llm_task_str == "1"
run_bqsr = run_bqsr_str == "1"
timeout_ms = int(timeout_ms_str)

schema = {
    "type": "object",
    "properties": {
        "ok": {"type": "boolean"},
        "reference_header_present": {"type": "boolean"},
        "outputs": {
            "type": "object",
            "properties": {
                "vcf": {"type": "string"},
                "report": {"type": "string"},
            },
            "required": ["vcf", "report"],
            "additionalProperties": False,
        },
        "checks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "pass": {"type": "boolean"},
                    "detail": {"type": "string"},
                },
                "required": ["name", "pass", "detail"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["ok", "reference_header_present", "outputs", "checks"],
    "additionalProperties": False,
}

if use_llm_task:
    llm_args = {
        "prompt": "你是生信流程验收器。只输出符合 schema 的 JSON。检查：1) VCF 头必须有 ##reference=；2) 输出文件路径存在；3) 关键统计字段齐全。",
        "input": {"note": "请基于上一阶段输出进行结构化验收"},
        "schema": schema,
    }
    base_cmd = f"python3 run_bioinformatics_analysis.py --fastq {fastq_path} --ref {ref_path} --outdir {outdir_path} --known-sites {known_sites_path}"
    if run_bqsr:
        base_cmd += " --run-bqsr"
        
    pipeline = (
        "exec --json --shell "
        f"'{base_cmd}' "
        f"| clawd.invoke --url http://127.0.0.1:18789 --token {token} "
        "--tool llm-task --action json --args-json "
        f"'{json.dumps(llm_args, ensure_ascii=False, separators=(',', ':'))}'"
    )
else:
    # Stable mode: avoid llm-task schema variability.
    base_cmd = f"python3 run_bioinformatics_analysis.py --fastq {fastq_path} --ref {ref_path} --outdir {outdir_path} --known-sites {known_sites_path}"
    if run_bqsr:
        base_cmd += " --run-bqsr"
        
    pipeline = (
        "exec --json --shell "
        f"'{base_cmd}'"
    )

payload = {
    "tool": "lobster",
    "args": {
        "action": "run",
        "cwd": ".",
        "timeoutMs": timeout_ms,
        "maxStdoutBytes": 1048576,
        "pipeline": pipeline,
    },
}

with open(payload_file, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)

print(f"[INFO] payload generated: {payload_file}")
print(f"[INFO] mode: {'llm-task-validation' if use_llm_task else 'stable'}")
print(f"[INFO] timeoutMs: {timeout_ms}")
PY

echo "[INFO] invoking generated payload..."
"${ROOT_DIR}/run_payload.sh" "$PAYLOAD_FILE" "$OUT_FILE"
