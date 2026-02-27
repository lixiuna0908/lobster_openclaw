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
from Crypto.Cipher import AES
from fastapi import FastAPI, Header, HTTPException, Request


ROOT_DIR = Path("/Users/work/000code/github")
RUN_SCRIPT = ROOT_DIR / "run_bio_payload.sh"
RUNTIME_DIR = ROOT_DIR / "dingtalk_runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

SIGN_SECRET = os.getenv("DINGTALK_SIGN_SECRET", "").strip()
CALLBACK_TOKEN = os.getenv("DINGTALK_CALLBACK_TOKEN", "").strip()
CALLBACK_AES_KEY = os.getenv("DINGTALK_CALLBACK_AES_KEY", "").strip()
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


def _sha1_signature(*parts: str) -> str:
    s = "".join(sorted(parts))
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _pkcs7_pad(data: bytes, block_size: int = 32) -> bytes:
    pad_len = block_size - (len(data) % block_size)
    return data + bytes([pad_len]) * pad_len


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        raise ValueError("empty data")
    pad_len = data[-1]
    if pad_len < 1 or pad_len > 32:
        raise ValueError("invalid PKCS7 padding")
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        raise ValueError("bad PKCS7 padding bytes")
    return data[:-pad_len]


def _decode_aes_key(aes_key_43: str) -> bytes:
    if not aes_key_43:
        raise ValueError("DINGTALK_CALLBACK_AES_KEY is empty")
    # DingTalk provides 43-char Base64 without trailing "="
    return base64.b64decode(aes_key_43 + "=")


def _decrypt_dingtalk_event(encrypt_b64: str, aes_key: bytes) -> Tuple[str, str]:
    encrypted = base64.b64decode(encrypt_b64)
    cipher = AES.new(aes_key, AES.MODE_CBC, iv=aes_key[:16])
    raw = _pkcs7_unpad(cipher.decrypt(encrypted))
    if len(raw) < 20:
        raise ValueError("decrypted payload too short")
    msg_len = int.from_bytes(raw[16:20], "big")
    msg = raw[20 : 20 + msg_len]
    receive_id = raw[20 + msg_len :].decode("utf-8", errors="replace")
    return msg.decode("utf-8", errors="replace"), receive_id


def _encrypt_dingtalk_event(plain_text: str, receive_id: str, aes_key: bytes) -> str:
    plain = plain_text.encode("utf-8")
    rid = (receive_id or "").encode("utf-8")
    body = os.urandom(16) + len(plain).to_bytes(4, "big") + plain + rid
    cipher = AES.new(aes_key, AES.MODE_CBC, iv=aes_key[:16])
    encrypted = cipher.encrypt(_pkcs7_pad(body))
    return base64.b64encode(encrypted).decode("utf-8")


def _encrypted_success_response(receive_id: str, timestamp: str, nonce: str) -> Dict[str, Any]:
    aes_key = _decode_aes_key(CALLBACK_AES_KEY)
    encrypted = _encrypt_dingtalk_event("success", receive_id, aes_key)
    sign = _sha1_signature(CALLBACK_TOKEN, timestamp, nonce, encrypted)
    return {
        "msg_signature": sign,
        "encrypt": encrypted,
        "timeStamp": timestamp,
        "nonce": nonce,
    }


def _decrypt_if_needed(request_payload: Dict[str, Any], query_params: Dict[str, str]) -> Tuple[Dict[str, Any], Optional[str], bool]:
    encrypt = request_payload.get("encrypt")
    if not encrypt:
        return request_payload, None, False

    if not CALLBACK_TOKEN or not CALLBACK_AES_KEY:
        raise HTTPException(status_code=500, detail="callback token/aes_key not configured")

    timestamp = str(query_params.get("timestamp") or query_params.get("timeStamp") or "")
    nonce = str(query_params.get("nonce") or "")
    signature = str(query_params.get("signature") or query_params.get("msg_signature") or "")
    if not timestamp or not nonce or not signature:
        raise HTTPException(status_code=400, detail="missing callback signature params")

    expected = _sha1_signature(CALLBACK_TOKEN, timestamp, nonce, str(encrypt))
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="invalid callback signature")

    aes_key = _decode_aes_key(CALLBACK_AES_KEY)
    plain_text, receive_id = _decrypt_dingtalk_event(str(encrypt), aes_key)
    try:
        decrypted_payload = json.loads(plain_text)
    except Exception:
        decrypted_payload = {"text": {"content": plain_text}}
    return decrypted_payload, receive_id, True


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
    # Optional legacy header-sign verification: only enforce when headers are present.
    # DingTalk event callbacks use query params (signature/msg_signature/timestamp/nonce),
    # which are verified in _decrypt_if_needed().
    if (timestamp or sign) and not _verify_signature(timestamp, sign):
        raise HTTPException(status_code=401, detail="invalid header signature")

    raw_payload = await request.json()
    query = {k: v for k, v in request.query_params.items()}
    payload, receive_id, is_encrypted_event = _decrypt_if_needed(raw_payload, query)

    # DingTalk callback URL verification and event acks should return encrypted success.
    event_type = str(payload.get("EventType") or payload.get("eventType") or "").lower()
    if is_encrypted_event and event_type in {"check_url", "check_create_suite_url", "url_check"}:
        ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
        nonce = str(query.get("nonce") or "nonce")
        return _encrypted_success_response(receive_id or "", ts, nonce)

    text = _normalize_text(payload)
    if not text:
        if is_encrypted_event:
            ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
            nonce = str(query.get("nonce") or "nonce")
            return _encrypted_success_response(receive_id or "", ts, nonce)
        return _reply_text("未识别到文本内容。")

    if not _contains_keyword(text):
        if is_encrypted_event:
            ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
            nonce = str(query.get("nonce") or "nonce")
            return _encrypted_success_response(receive_id or "", ts, nonce)
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
        if is_encrypted_event:
            ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
            nonce = str(query.get("nonce") or "nonce")
            # For encrypted event callbacks, respond with encrypted success and send actual text via webhook.
            _send_dingtalk_text(webhook, msg)
            return _encrypted_success_response(receive_id or "", ts, nonce)
        return _reply_text(msg)

    # Run command
    if not state.fastq or not state.ref:
        if is_encrypted_event:
            ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
            nonce = str(query.get("nonce") or "nonce")
            _send_dingtalk_text(webhook, "缺少参数。请先发送 fastq=... 和 ref=...，再发送“帮我运行”。")
            return _encrypted_success_response(receive_id or "", ts, nonce)
        return _reply_text("缺少参数。请先发送 fastq=... 和 ref=...，再发送“帮我运行”。")
    if not Path(state.fastq).exists():
        if is_encrypted_event:
            ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
            nonce = str(query.get("nonce") or "nonce")
            _send_dingtalk_text(webhook, f"FASTQ 路径不存在：{state.fastq}")
            return _encrypted_success_response(receive_id or "", ts, nonce)
        return _reply_text(f"FASTQ 路径不存在：{state.fastq}")
    if not Path(state.ref).exists():
        if is_encrypted_event:
            ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
            nonce = str(query.get("nonce") or "nonce")
            _send_dingtalk_text(webhook, f"参考基因组路径不存在：{state.ref}")
            return _encrypted_success_response(receive_id or "", ts, nonce)
        return _reply_text(f"参考基因组路径不存在：{state.ref}")
    if not RUN_SCRIPT.exists():
        if is_encrypted_event:
            ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
            nonce = str(query.get("nonce") or "nonce")
            _send_dingtalk_text(webhook, f"执行脚本不存在：{RUN_SCRIPT}")
            return _encrypted_success_response(receive_id or "", ts, nonce)
        return _reply_text(f"执行脚本不存在：{RUN_SCRIPT}")

    t = threading.Thread(target=_run_pipeline_async, args=(session_key, webhook, state), daemon=True)
    t.start()
    if is_encrypted_event:
        ts = str(query.get("timestamp") or query.get("timeStamp") or str(int(time.time() * 1000)))
        nonce = str(query.get("nonce") or "nonce")
        _send_dingtalk_text(webhook, "已接收运行请求，任务开始执行。稍后会回传结果。")
        return _encrypted_success_response(receive_id or "", ts, nonce)
    return _reply_text("已接收运行请求，任务开始执行。稍后会回传结果。")


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("DINGTALK_BRIDGE_HOST", "0.0.0.0")
    port = int(os.getenv("DINGTALK_BRIDGE_PORT", "8788"))
    uvicorn.run("dingtalk_bridge:app", host=host, port=port, reload=False)
