#!/usr/bin/env python3
import base64
import hashlib
import hmac
import json
import os
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote_plus

import requests
from fastapi import FastAPI, Header, HTTPException, Request


ROOT_DIR = Path("/Users/work/000code/github")
RUN_SCRIPT = ROOT_DIR / "run_bio_payload.sh"
RUNTIME_DIR = ROOT_DIR / "dingtalk_runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

SIGN_SECRET = os.getenv("DINGTALK_SIGN_SECRET", "").strip()
DEFAULT_REPLY_WEBHOOK = os.getenv("DINGTALK_REPLY_WEBHOOK", "").strip()
KEYWORDS = [
    kw.strip() for kw in os.getenv("DINGTALK_KEYWORDS", "帮我运行,运行流程,开始处理,FASTQ,ref").split(",") if kw.strip()
]
RUN_TIMEOUT_SEC = int(os.getenv("BIO_RUN_TIMEOUT_SEC", "3600"))

app = FastAPI(title="DingTalk Bioinformatics Bridge")


@dataclass
class SessionState:
    fastq: Optional[str] = None
    ref: Optional[str] = None
    outdir: Optional[str] = None
    last_text: Optional[str] = None
    updated_at: float = field(default_factory=time.time)


SESSIONS: Dict[str, SessionState] = {}


def _safe_session_key(payload: Dict[str, Any]) -> str:
    return (
        str(
            payload.get("conversationId")
            or payload.get("chatId")
            or payload.get("sessionWebhook")
            or payload.get("senderStaffId")
            or payload.get("senderId")
            or "default"
        )
        .strip()
        .replace("\n", "_")
    )


def _normalize_text(payload: Dict[str, Any]) -> str:
    text = ""
    if isinstance(payload.get("text"), dict):
        text = str(payload["text"].get("content") or "")
    if not text:
        text = str(payload.get("content") or payload.get("msg") or payload.get("message") or "")
    return text.strip()


def _contains_keyword(text: str) -> bool:
    lower = text.lower()
    return any(kw.lower() in lower for kw in KEYWORDS)


def _verify_signature(timestamp: Optional[str], sign: Optional[str]) -> bool:
    if not SIGN_SECRET:
        return True
    if not timestamp or not sign:
        return False
    string_to_sign = f"{timestamp}\n{SIGN_SECRET}"
    digest = hmac.new(SIGN_SECRET.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    # Some clients URL-encode sign
    got = unquote_plus(sign)
    return hmac.compare_digest(expected, got)


def _extract_paths(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    patterns = {
        "fastq": r"(?:fastq)\s*[:=：]\s*([^\s]+)",
        "ref": r"(?:ref|reference|参考(?:基因组)?)\s*[:=：]\s*([^\s]+)",
        "outdir": r"(?:outdir|output|输出目录)\s*[:=：]\s*([^\s]+)",
    }
    fastq = None
    ref = None
    outdir = None
    m = re.search(patterns["fastq"], text, flags=re.IGNORECASE)
    if m:
        fastq = m.group(1).strip()
    m = re.search(patterns["ref"], text, flags=re.IGNORECASE)
    if m:
        ref = m.group(1).strip()
    m = re.search(patterns["outdir"], text, flags=re.IGNORECASE)
    if m:
        outdir = m.group(1).strip()
    return fastq, ref, outdir


def _should_run(text: str) -> bool:
    run_words = ["帮我运行", "运行流程", "开始处理", "开始运行", "run", "执行"]
    lower = text.lower()
    return any(w.lower() in lower for w in run_words)


def _send_dingtalk_text(webhook: str, content: str) -> None:
    if not webhook:
        return
    try:
        requests.post(
            webhook,
            json={"msgtype": "text", "text": {"content": content}},
            timeout=10,
        )
    except Exception:
        pass


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _run_pipeline_async(session_key: str, webhook: str, state: SessionState) -> None:
    ts = time.strftime("%Y%m%d_%H%M%S")
    payload_path = RUNTIME_DIR / f"{session_key}_{ts}_payload.json"
    result_path = RUNTIME_DIR / f"{session_key}_{ts}_result.json"
    outdir = state.outdir or str(ROOT_DIR / "test_data" / "out_dingtalk")

    env = os.environ.copy()
    env["FASTQ_PATH"] = state.fastq or ""
    env["REF_PATH"] = state.ref or ""
    env["OUTDIR_PATH"] = outdir

    cmd = [str(RUN_SCRIPT), str(payload_path), str(result_path)]
    start_msg = (
        "已开始执行生信流程。\n"
        f"FASTQ: {env['FASTQ_PATH']}\n"
        f"REF: {env['REF_PATH']}\n"
        f"OUTDIR: {env['OUTDIR_PATH']}"
    )
    _send_dingtalk_text(webhook, start_msg)

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=RUN_TIMEOUT_SEC,
        )
        result_json = _load_json(result_path)
        prediction_path = Path(outdir) / "disease_prediction.json"
        prediction = _load_json(prediction_path)
        if proc.returncode == 0:
            content = (
                "流程执行完成。\n"
                f"VCF: {Path(outdir) / 'sample1.variants.vcf'}\n"
                f"CSV: {Path(outdir) / 'mutations.csv'}\n"
                f"报告: {Path(outdir) / 'disease_association_report.md'}\n"
                f"风险等级: {prediction.get('overall_risk_level', 'unknown')}\n"
                f"变异数: {prediction.get('variant_count', 'unknown')}\n"
                f"结果文件: {result_path}"
            )
            _send_dingtalk_text(webhook, content)
        else:
            err = (proc.stderr or proc.stdout or "").strip()[-1200:]
            _send_dingtalk_text(webhook, f"流程失败（exit={proc.returncode}）。\n{err}\n结果文件: {result_path}")
    except subprocess.TimeoutExpired:
        _send_dingtalk_text(webhook, f"流程超时（>{RUN_TIMEOUT_SEC}s）。")
    except Exception as e:
        _send_dingtalk_text(webhook, f"流程异常：{e}")


def _reply_text(content: str) -> Dict[str, Any]:
    return {"msgtype": "text", "text": {"content": content}}


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"ok": "true"}


@app.post("/dingtalk/callback")
async def dingtalk_callback(
    request: Request,
    timestamp: Optional[str] = Header(default=None),
    sign: Optional[str] = Header(default=None),
):
    if not _verify_signature(timestamp, sign):
        raise HTTPException(status_code=401, detail="invalid signature")

    payload = await request.json()
    text = _normalize_text(payload)
    if not text:
        return _reply_text("未识别到文本内容。")

    if not _contains_keyword(text):
        return _reply_text("已接收。提示：请包含关键字（如 FASTQ/ref/帮我运行）。")

    session_key = _safe_session_key(payload)
    state = SESSIONS.get(session_key) or SessionState()
    state.last_text = text
    state.updated_at = time.time()

    fastq, ref, outdir = _extract_paths(text)
    if fastq:
        state.fastq = fastq
    if ref:
        state.ref = ref
    if outdir:
        state.outdir = outdir
    SESSIONS[session_key] = state

    webhook = (
        str(payload.get("sessionWebhook") or payload.get("conversationWebhook") or DEFAULT_REPLY_WEBHOOK).strip()
    )

    # Preview / parameters update
    if not _should_run(text):
        msg = (
            "参数已记录。\n"
            f"FASTQ: {state.fastq or '未设置'}\n"
            f"REF: {state.ref or '未设置'}\n"
            f"OUTDIR: {state.outdir or str(ROOT_DIR / 'test_data' / 'out_dingtalk')}\n"
            "发送“帮我运行”即可开始。"
        )
        return _reply_text(msg)

    # Run command
    if not state.fastq or not state.ref:
        return _reply_text("缺少参数。请先发送 fastq=... 和 ref=...，再发送“帮我运行”。")
    if not Path(state.fastq).exists():
        return _reply_text(f"FASTQ 路径不存在：{state.fastq}")
    if not Path(state.ref).exists():
        return _reply_text(f"参考基因组路径不存在：{state.ref}")
    if not RUN_SCRIPT.exists():
        return _reply_text(f"执行脚本不存在：{RUN_SCRIPT}")

    t = threading.Thread(target=_run_pipeline_async, args=(session_key, webhook, state), daemon=True)
    t.start()
    return _reply_text("已接收运行请求，任务开始执行。稍后会回传结果。")


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("DINGTALK_BRIDGE_HOST", "0.0.0.0")
    port = int(os.getenv("DINGTALK_BRIDGE_PORT", "8788"))
    uvicorn.run("dingtalk_bridge:app", host=host, port=port, reload=False)
