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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

try:
    import dingtalk_stream
    from dingtalk_stream import AckMessage
except ImportError as exc:
    raise SystemExit(
        "缺少 dingtalk-stream 依赖，请先执行: pip install -r requirements.txt"
    ) from exc


ROOT_DIR = Path("/Users/work/000code/github")
RUN_SCRIPT = ROOT_DIR / "run_bio_payload.sh"
RUNTIME_DIR = ROOT_DIR / "dingtalk_runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

CLIENT_ID = os.getenv("DINGTALK_STREAM_CLIENT_ID", "dingnidkqchjoxh6rr4j").strip()
CLIENT_SECRET = os.getenv("DINGTALK_STREAM_CLIENT_SECRET", "uTM3I164R1bhMmcqjvqwJOpoFkKT0pCxFVq8mkhJSJZjnD3IEkS0Hz_lFbEoiQ-f").strip()
DEFAULT_REPLY_WEBHOOK = os.getenv("DINGTALK_REPLY_WEBHOOK", "").strip()
REPLY_SIGN_SECRET = os.getenv("DINGTALK_REPLY_SIGN_SECRET", "").strip()
KEYWORDS = [
    kw.strip()
    for kw in os.getenv("DINGTALK_KEYWORDS", "帮我运行,运行流程,开始处理,FASTQ,ref,重新运行").split(",")
    if kw.strip()
]
RUN_TIMEOUT_SEC = int(os.getenv("BIO_RUN_TIMEOUT_SEC", "3600"))
LOG_ALL_TOPICS = os.getenv("DINGTALK_STREAM_LOG_ALL_TOPICS", "1").strip() not in {"0", "false", "False"}


@dataclass
class SessionState:
    fastq: Optional[str] = None
    fastq2: Optional[str] = None
    ref: Optional[str] = None
    outdir: Optional[str] = None
    last_text: Optional[str] = None
    updated_at: float = field(default_factory=time.time)


SESSIONS: Dict[str, SessionState] = {}


def _safe_session_key(payload: Dict[str, Any]) -> str:
    raw = str(
        payload.get("conversationId")
        or payload.get("chatId")
        or payload.get("conversationType")
        or payload.get("senderStaffId")
        or payload.get("senderId")
        or "default"
    ).strip()
    # Ensure the session key is always filesystem-safe for runtime artifacts.
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    return safe[:120] or "default"


def _normalize_text(payload: Dict[str, Any]) -> str:
    text = ""
    # Common chatbot payload shape.
    if isinstance(payload.get("text"), dict):
        text = str(payload["text"].get("content") or "")
    # Some stream callbacks may nest message body.
    if not text and isinstance(payload.get("message"), dict):
        message = payload.get("message") or {}
        if isinstance(message.get("text"), dict):
            text = str(message["text"].get("content") or "")
        if not text:
            text = str(message.get("content") or message.get("msg") or "")
    if not text:
        text = str(payload.get("content") or payload.get("msg") or payload.get("message") or "")
    return text.strip()


def _contains_keyword(text: str) -> bool:
    lower = text.lower()
    return any(kw.lower() in lower for kw in KEYWORDS)


def _extract_paths(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    patterns = {
        "fastq": r"(?:fastq1?)\s*[:=：]\s*([^\s]+)",
        "fastq2": r"(?:fastq2)\s*[:=：]\s*([^\s]+)",
        "ref": r"(?:ref|reference|参考(?:基因组)?)\s*[:=：]\s*([^\s]+)",
        "outdir": r"(?:outdir|output|输出目录)\s*[:=：]\s*([^\s]+)",
    }
    fastq = None
    fastq2 = None
    ref = None
    outdir = None
    m = re.search(patterns["fastq"], text, flags=re.IGNORECASE)
    if m:
        fastq = m.group(1).strip()
    m = re.search(patterns["fastq2"], text, flags=re.IGNORECASE)
    if m:
        fastq2 = m.group(1).strip()
    m = re.search(patterns["ref"], text, flags=re.IGNORECASE)
    if m:
        ref = m.group(1).strip()
    m = re.search(patterns["outdir"], text, flags=re.IGNORECASE)
    if m:
        outdir = m.group(1).strip()
    return fastq, fastq2, ref, outdir


def _should_run(text: str) -> bool:
    run_words = ["帮我运行", "运行流程", "开始处理", "开始运行", "run", "执行", "重新运行"]
    lower = text.lower()
    return any(w.lower() in lower for w in run_words)


def _send_dingtalk_text(webhook: str, content: str) -> None:
    if not webhook:
        return
    try:
        target_webhook = _build_signed_webhook(webhook)
        if "<font" in content or "##" in content:
            # Auto-detect markdown
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "生信流程通知",
                    "text": content.replace("\n", " \n\n")
                }
            }
        else:
            payload = {
                "msgtype": "text",
                "text": {"content": content}
            }
        requests.post(
            target_webhook,
            json=payload,
            timeout=10,
        )
    except Exception:
        pass


def _build_signed_webhook(webhook: str) -> str:
    if not webhook or not REPLY_SIGN_SECRET:
        return webhook
    timestamp = str(int(time.time() * 1000))
    to_sign = f"{timestamp}\n{REPLY_SIGN_SECRET}"
    digest = hmac.new(
        REPLY_SIGN_SECRET.encode("utf-8"),
        to_sign.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    sign = base64.b64encode(digest).decode("utf-8")

    parsed = urlsplit(webhook)
    query_pairs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k not in {"timestamp", "sign"}
    ]
    query_pairs.extend([("timestamp", timestamp), ("sign", sign)])
    signed_query = urlencode(query_pairs)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, signed_query, parsed.fragment))


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _fmt_duration(seconds: float) -> str:
    """格式化耗时为「时/分/秒」或「秒/毫秒」，精确到毫秒。"""
    total = max(0.0, float(seconds))
    if total >= 3600:
        hours = int(total // 3600)
        rem = total % 3600
        minutes = int(rem // 60)
        s = rem % 60
        return f"{hours}小时{minutes}分{s:.3f}秒"
    if total >= 60:
        minutes = int(total // 60)
        s = total % 60
        return f"{minutes}分{s:.3f}秒"
    if total >= 1:
        return f"{total:.3f}秒"
    if total > 0:
        return f"{total * 1000:.1f}毫秒"
    return "0毫秒"


STAGE_LABELS: Dict[int, str] = {
    2: "参考索引构建",
    3: "FASTQ 比对到参考基因组，输出 SAM",
    4: "SAM -> sorted BAM（排序 BAM）",
    5: "给 BAM 建索引及质量控制（BQSR / QC）",
    6: "变异检测，输出 VCF（GATK HaplotypeCaller）",
    7: "VCF 处理及变异过滤 (CNN 机器学习或硬过滤)",
    8: "VCF -> mutations.csv",
    9: "基于 CSV 做风险计算（总体风险等级 + 各疾病分数）",
    10: "生成最终报告",
}

STAGE_ESTIMATES: Dict[int, float] = {
    2: 1.5,   # 参考索引构建约 1.5 小时 (hg38 bwa index)
    3: 0.5,   # BWA mem 约 0.5 小时
    4: 0.2,   # Sort BAM 约 0.2 小时
    5: 0.1,   # Index BAM 约 0.1 小时
    6: 1.0,   # HaplotypeCaller 约 1.0 小时
    7: 0.1,   # Header check 约 0.1 小时
    8: 0.1,   # CSV conversion 约 0.1 小时
    9: 0.1,   # Risk calculation 约 0.1 小时
    10: 0.1,  # Report gen 约 0.1 小时
}

NODE_TO_STAGE: Dict[str, int] = {
    "build_reference_index": 2,
    "align_reads_bwa_mem": 3,
    "sort_bam_samtools": 4,
    "index_bam_samtools": 5,
    "alignment_qc": 5,
    "bqsr_recalibration": 5,
    "index_recal_bam": 5,
    "call_variants_gatk_haplotypecaller": 6,
    "ensure_vcf_reference_header": 7,
    "filter_variants_hard": 7,
    "nv_score_variants": 7,
    "filter_variant_tranches": 7,
    "ensure_filtered_vcf_reference_header": 7,
    "convert_vcf_to_csv": 8,
    "disease_prediction_from_csv": 9,
    "generate_markdown_report": 10,
    "cleanup_temp_files": 10,
}


def _node_duration_seconds(node: Dict[str, Any]) -> float:
    """优先用 started_at 与 finished_at 计算实际耗时（秒），否则用 duration_ms。"""
    started = node.get("started_at")
    finished = node.get("finished_at")
    if started and finished:
        try:
            t0 = datetime.fromisoformat(started.replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(finished.replace("Z", "+00:00"))
            if t0.tzinfo is None:
                t0 = t0.replace(tzinfo=timezone.utc)
            if t1.tzinfo is None:
                t1 = t1.replace(tzinfo=timezone.utc)
            return max(0.0, (t1 - t0).total_seconds())
        except (ValueError, TypeError):
            pass
    return node.get("duration_ms", 0) / 1000.0


def _load_stage_progress(outdir_path: Path, stage2_progress: int) -> Tuple[Dict[int, int], Dict[int, float]]:
    progress: Dict[int, int] = {k: 0 for k in range(2, 11)}
    durations: Dict[int, float] = {k: 0.0 for k in range(2, 11)}
    progress[2] = max(0, min(100, int(stage2_progress)))
    records = _load_json(outdir_path / "pipeline_node_records.json")
    nodes = records.get("nodes")
    if not isinstance(nodes, list):
        return progress, durations
    
    # 记录已完成或正在运行的 stage；实际耗时由 started_at ~ finished_at 计算
    active_stages = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        stage = NODE_TO_STAGE.get(str(node.get("name") or ""))
        if not stage:
            continue
        
        status = str(node.get("status") or "")
        durations[stage] += _node_duration_seconds(node)
        
        if status == "ok":
            progress[stage] = 100
            active_stages.add(stage)
        elif status == "running":
            if progress[stage] == 0:
                progress[stage] = 1
            active_stages.add(stage)
            
    # 修正逻辑：如果 stage N 在运行或已完成，它之前的 stage 必须是 100%
    if active_stages:
        max_active_stage = max(active_stages)
        for s in range(2, max_active_stage):
            progress[s] = 100
            
    return progress, durations


def _build_stage_status_text(
    *,
    stage_progress: Dict[int, int],
    stage_durations: Dict[int, float],
    total_started_at: float,
    running: bool,
    finished_ok: bool,
) -> str:
    now = time.time()
    total_elapsed = _fmt_duration(now - total_started_at)
    lines = [f"流程进度看板（总耗时：{total_elapsed}）："]
    for stage in range(2, 11):
        pct = 100 if finished_ok else max(0, min(100, int(stage_progress.get(stage, 0))))
        label = STAGE_LABELS[stage]
        est = STAGE_ESTIMATES.get(stage, 0.1)
        display_idx = stage - 1
        if pct >= 100:
            dur = stage_durations.get(stage, 0.0)
            lines.append(f"<font color=\"#00A600\">{display_idx}、{label}：100%（实际耗时：{_fmt_duration(dur)}）</font>")
        elif pct <= 0:
            lines.append(f"{display_idx}、{label}：0%（未开始，预计耗时：约{est}小时）")
        else:
            suffix = f"预计耗时：约{est}小时" if running else "已中断"
            lines.append(f"{display_idx}、{label}：{pct}%（{suffix}）")
    return "\n".join(lines)


def _dump_incoming_payload(raw: Any, payload: Dict[str, Any]) -> None:
    ts = time.strftime("%Y%m%d_%H%M%S")
    millis = int((time.time() % 1) * 1000)
    dump_path = RUNTIME_DIR / f"incoming_{ts}_{millis:03d}.json"
    record = {
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "raw_type": type(raw).__name__,
        "raw": raw,
        "parsed_type": type(payload).__name__,
        "parsed_payload": payload,
        "normalized_text": _normalize_text(payload),
    }
    try:
        dump_path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[DEBUG] payload dumped: {dump_path}", flush=True)
        # 便于排查“钉钉发了但没推送”：每次收到消息写一行，可查最后收到时间
        try:
            (RUNTIME_DIR / "last_message_received.log").write_text(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} incoming={dump_path.name}\n",
                encoding="utf-8",
            )
        except Exception:
            pass
    except Exception as exc:
        print(f"[WARN] payload dump failed: {exc}", flush=True)


def _run_pipeline_async(session_key: str, webhook: str, state: SessionState) -> None:
    ts = time.strftime("%Y%m%d_%H%M%S")
    payload_path = RUNTIME_DIR / f"{session_key}_{ts}_payload.json"
    result_path = RUNTIME_DIR / f"{session_key}_{ts}_result.json"
    outdir = state.outdir or str(ROOT_DIR / "test_data" / "out_dingtalk")

    env = os.environ.copy()
    env["FASTQ_PATH"] = state.fastq or ""
    env["FASTQ2_PATH"] = state.fastq2 or ""
    env["REF_PATH"] = state.ref or ""
    env["OUTDIR_PATH"] = outdir

    cmd = [str(RUN_SCRIPT), str(payload_path), str(result_path)]
    outdir_path = Path(outdir)
    progress_file = outdir_path / "bwa_index_progress.json"
    records_file = outdir_path / "pipeline_node_records.json"
    
    # 清除历史进度记录，防止“重新运行”时读取到上一轮100%的假象
    if progress_file.exists():
        progress_file.unlink(missing_ok=True)
    if records_file.exists():
        records_file.unlink(missing_ok=True)

    initial_progress, initial_durations = _load_stage_progress(outdir_path, 0)
    start_msg = (
        "已开始执行生信流程。\n"
        f"FASTQ: {env['FASTQ_PATH']}\n"
        f"FASTQ2: {env['FASTQ2_PATH'] or '未设置(单端)'}\n"
        f"REF: {env['REF_PATH']}\n"
        f"OUTDIR: {env['OUTDIR_PATH']}\n\n"
        + _build_stage_status_text(
            stage_progress=initial_progress,
            stage_durations=initial_durations,
            total_started_at=time.time(),
            running=True,
            finished_ok=False,
        )
    )
    _send_dingtalk_text(webhook, start_msg)

    try:
        stdout_file = RUNTIME_DIR / f"{session_key}_{ts}_stdout.log"
        stderr_file = RUNTIME_DIR / f"{session_key}_{ts}_stderr.log"
        with stdout_file.open("w", encoding="utf-8") as out_fp, stderr_file.open("w", encoding="utf-8") as err_fp:
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT_DIR),
                env=env,
                stdout=out_fp,
                stderr=err_fp,
                text=True,
            )
            start_ts = time.time()
            last_sent_bwa_prog = 0
            last_reported_stages_hash = ""
            
            while True:
                rc = proc.poll()
                prog_data = _load_json(progress_file)
                bwa_prog = prog_data.get("progress") if prog_data else 0
                bwa_prog = int(bwa_prog) if isinstance(bwa_prog, (int, float)) else 0
                
                stage_progress, stage_durations = _load_stage_progress(outdir_path, bwa_prog)
                
                # 读取记录中的内部节点进度（例如 HaplotypeCaller 报上来的进度）
                node_prog = 0
                records = _load_json(records_file)
                nodes = records.get("nodes")
                if isinstance(nodes, list) and len(nodes) > 0:
                    current_node = nodes[-1]
                    if current_node.get("status") == "running" and "progress" in current_node:
                        node_prog = current_node.get("progress", 0)
                        
                        # 如果是变异检测节点，把它的进度更新到 stage_progress 的第6步
                        stage = NODE_TO_STAGE.get(str(current_node.get("name") or ""))
                        if stage and stage in stage_progress and stage_progress[stage] < 100:
                            stage_progress[stage] = node_prog
                            
                other_stages_hash = str([(k, stage_progress[k]) for k in sorted(stage_progress.keys()) if k != 2])
                
                should_push = False
                if other_stages_hash != last_reported_stages_hash:
                    should_push = True
                elif bwa_prog >= last_sent_bwa_prog + 5:
                    should_push = True
                elif node_prog > 0 and node_prog >= last_sent_bwa_prog + 5:
                    # 借用 last_sent_bwa_prog 变量名作为通用的进度记录间隔(5%)
                    should_push = True
                    
                # 如果是第一条进度，即使 hash 没变也推送一次真实进度，但要确保不推送全是0的状态
                if last_reported_stages_hash == "" and (other_stages_hash != "[]" or bwa_prog > 0 or node_prog > 0):
                    should_push = True
                
                if should_push:
                    # 避免在完全没有开始时推送空状态
                    if not (stage_progress.get(2, 0) == 0 and sum(stage_progress.values()) == 0):
                        panel = _build_stage_status_text(
                            stage_progress=stage_progress,
                            stage_durations=stage_durations,
                            total_started_at=start_ts,
                            running=True,
                            finished_ok=False,
                        )
                        _send_dingtalk_text(webhook, panel)
                        last_reported_stages_hash = other_stages_hash
                        if bwa_prog >= last_sent_bwa_prog + 5:
                            last_sent_bwa_prog = (bwa_prog // 5) * 5
                        elif node_prog > 0 and node_prog >= last_sent_bwa_prog + 5:
                            last_sent_bwa_prog = (node_prog // 5) * 5
                    else:
                        # 抑制了这次推送，重置状态以便后续可以正常推送
                        last_reported_stages_hash = ""
                
                if rc is not None:
                    break
                if time.time() - start_ts > RUN_TIMEOUT_SEC:
                    proc.kill()
                    raise subprocess.TimeoutExpired(cmd, RUN_TIMEOUT_SEC)
                time.sleep(3)

        rc = proc.returncode if proc.returncode is not None else -1
        proc_stdout = stdout_file.read_text(encoding="utf-8", errors="replace")
        proc_stderr = stderr_file.read_text(encoding="utf-8", errors="replace")

        prediction_path = Path(outdir) / "disease_prediction.json"
        prediction = _load_json(prediction_path)
        if rc == 0:
            top_risks = prediction.get("predictions", [])[:5]
            risk_lines = "\n".join([f"- {item['disease']}: {item['score']}" for item in top_risks])
            content = (
                "流程执行完成。\n"
                f"VCF: {Path(outdir) / 'sample1.variants.vcf'}\n"
                f"CSV: {Path(outdir) / 'mutations.csv'}\n"
                f"报告: {Path(outdir) / 'disease_association_report.md'}\n"
                f"风险等级: {prediction.get('overall_risk_level', 'unknown')}\n"
                f"综合得分: {prediction.get('overall_score', 'unknown')}\n"
                f"变异数: {prediction.get('variant_count', 'unknown')}\n"
                f"主要预测风险 (Top 5):\n{risk_lines}\n"
                f"结果文件: {result_path}"
            )
            final_progress, final_durations = _load_stage_progress(outdir_path, 100)
            final_panel = _build_stage_status_text(
                stage_progress={k: 100 for k in range(2, 11)},
                stage_durations=final_durations,
                total_started_at=start_ts,
                running=False,
                finished_ok=True,
            )
            _send_dingtalk_text(webhook, f"{final_panel}\n\n{content}")
        else:
            err = (proc_stderr or proc_stdout or "").strip()[-1200:]
            gateway_err = ""
            try:
                if result_path.exists():
                    res = _load_json(result_path)
                    if isinstance(res, dict) and res.get("ok") is False:
                        e = res.get("error")
                        if isinstance(e, dict) and isinstance(e.get("message"), str):
                            gateway_err = e["message"].strip()
            except Exception:
                pass
            err_line = f"网关错误: {gateway_err}\n" if gateway_err else ""
            last_progress = int(_load_json(progress_file).get("progress") or 0)
            failed_progress, failed_durations = _load_stage_progress(outdir_path, last_progress)
            failed_panel = _build_stage_status_text(
                stage_progress=failed_progress,
                stage_durations=failed_durations,
                total_started_at=start_ts,
                running=False,
                finished_ok=False,
            )
            _send_dingtalk_text(webhook, f"{failed_panel}\n\n流程失败（exit={rc}）。\n{err_line}{err}\n结果文件: {result_path}")
    except subprocess.TimeoutExpired:
        _send_dingtalk_text(webhook, f"流程超时（>{RUN_TIMEOUT_SEC}s）。")
    except Exception as exc:
        _send_dingtalk_text(webhook, f"流程异常：{exc}")


def _handle_message(payload: Dict[str, Any]) -> None:
    text = _normalize_text(payload)
    if not text:
        return
    if not _contains_keyword(text):
        return

    session_key = _safe_session_key(payload)
    state = SESSIONS.get(session_key) or SessionState()
    state.last_text = text
    state.updated_at = time.time()

    fastq, fastq2, ref, outdir = _extract_paths(text)
    if fastq:
        state.fastq = fastq
    if fastq2:
        state.fastq2 = fastq2
    if ref:
        state.ref = ref
    if outdir:
        state.outdir = outdir
    SESSIONS[session_key] = state

    webhook = str(payload.get("sessionWebhook") or payload.get("conversationWebhook") or DEFAULT_REPLY_WEBHOOK).strip()

    if not _should_run(text):
        msg = (
            "参数已记录。\n"
            f"FASTQ: {state.fastq or '未设置'}\n"
            f"FASTQ2: {state.fastq2 or '未设置(单端)'}\n"
            f"REF: {state.ref or '未设置'}\n"
            f"OUTDIR: {state.outdir or str(ROOT_DIR / 'test_data' / 'out_dingtalk')}\n"
            "发送“帮我运行”即可开始。"
        )
        _send_dingtalk_text(webhook, msg)
        return

    if not state.fastq or not state.ref:
        _send_dingtalk_text(webhook, "缺少参数。请先发送 fastq=... 和 ref=...，再发送“帮我运行”。")
        return
    if not Path(state.fastq).exists():
        _send_dingtalk_text(webhook, f"FASTQ 路径不存在：{state.fastq}")
        return
    if state.fastq2 and not Path(state.fastq2).exists():
        _send_dingtalk_text(webhook, f"FASTQ2 路径不存在：{state.fastq2}")
        return
    if not Path(state.ref).exists():
        _send_dingtalk_text(webhook, f"参考基因组路径不存在：{state.ref}")
        return
    if not RUN_SCRIPT.exists():
        _send_dingtalk_text(webhook, f"执行脚本不存在：{RUN_SCRIPT}")
        return

    thread = threading.Thread(target=_run_pipeline_async, args=(session_key, webhook, state), daemon=True)
    thread.start()
    _send_dingtalk_text(webhook, "已接收运行请求，任务开始执行。稍后会回传结果。")


class BioChatbotHandler(dingtalk_stream.ChatbotHandler):
    async def process(self, callback):  # type: ignore[override]
        payload: Dict[str, Any] = {}
        raw: Any = {}
        try:
            raw = callback.data if hasattr(callback, "data") else {}
            if isinstance(raw, dict):
                payload = raw
            elif isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    payload = parsed if isinstance(parsed, dict) else {}
                except Exception:
                    payload = {}
            else:
                payload = {}

            _dump_incoming_payload(raw, payload)
            # Debug key fields so we can diagnose non-standard message schemas quickly.
            norm_text = _normalize_text(payload)
            print(
                "[DEBUG] incoming message: "
                f"keys={list(payload.keys())[:12]} "
                f"msgtype={payload.get('msgtype')} "
                f"conversationType={payload.get('conversationType')} "
                f"text={norm_text[:180]!r}"
                ,
                flush=True,
            )
            _handle_message(payload)
        except Exception as exc:
            webhook = str(payload.get("sessionWebhook") or payload.get("conversationWebhook") or DEFAULT_REPLY_WEBHOOK).strip()
            _send_dingtalk_text(webhook, f"Stream 处理异常：{exc}")
        return AckMessage.STATUS_OK, "OK"


class DebugDingTalkStreamClient(dingtalk_stream.DingTalkStreamClient):
    async def route_message(self, json_message):  # type: ignore[override]
        if LOG_ALL_TOPICS:
            msg_type = str(json_message.get("type", ""))
            topic = str((json_message.get("headers") or {}).get("topic", ""))
            self.logger.info("[TOPIC] type=%s topic=%s", msg_type, topic)
        return await super().route_message(json_message)


def main() -> int:
    if not CLIENT_ID or not CLIENT_SECRET:
        print("[ERROR] 请设置 DINGTALK_STREAM_CLIENT_ID / DINGTALK_STREAM_CLIENT_SECRET")
        return 2
    if not DEFAULT_REPLY_WEBHOOK:
        print("[WARN] DINGTALK_REPLY_WEBHOOK 未设置，将依赖消息中的 sessionWebhook 回复。")
    print("[INFO] DingTalk Stream bridge starting...")
    credential = dingtalk_stream.Credential(CLIENT_ID, CLIENT_SECRET)
    client = DebugDingTalkStreamClient(credential)
    client.register_callback_handler(dingtalk_stream.chatbot.ChatbotMessage.TOPIC, BioChatbotHandler())
    client.start_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
