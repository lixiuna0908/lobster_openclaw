#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO


def _ensure_runtime_path() -> None:
    conda_bin = str(Path.home() / "miniconda3" / "bin")
    current = os.environ.get("PATH", "")
    parts = [p for p in current.split(os.pathsep) if p]
    if conda_bin in parts:
        return
    os.environ["PATH"] = conda_bin if not current else conda_bin + os.pathsep + current


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class NodeRecorder:
    def __init__(self, pipeline_id: str, sample_id: str, outdir: Path) -> None:
        self.pipeline_id = pipeline_id
        self.sample_id = sample_id
        self.outdir = outdir
        self.started_at = _now_iso()
        self.nodes: List[Dict[str, Any]] = []
        self._counter = 0

    def start(self, name: str, inputs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._counter += 1
        node = {
            "id": self._counter,
            "name": name,
            "status": "running",
            "started_at": _now_iso(),
            "_t0": time.time(),
            "inputs": inputs or {},
            "outputs": {},
            "stats": {},
            "commands": [],
        }
        self.nodes.append(node)
        self.write("running")  # 将刚启动的状态落盘，以便监控程序读取
        return node

    def finish(
        self,
        node: Dict[str, Any],
        *,
        status: str = "ok",
        outputs: Optional[Dict[str, Any]] = None,
        stats: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        node["status"] = status
        node["finished_at"] = _now_iso()
        node["duration_ms"] = int((time.time() - float(node.pop("_t0"))) * 1000)
        if outputs:
            node["outputs"] = outputs
        if stats:
            node["stats"] = stats
        if error:
            node["error"] = error
        self.write("running")  # 每次节点完成也立刻落盘

    def write_progress(self, status: str, progress: int) -> None:
        # 新增方法，用于报告当前活动节点的进度（比如 HaplotypeCaller 的5%）
        if not self.nodes:
            return
        # 假设最后一个节点是当前正在运行的节点
        current_node = self.nodes[-1]
        current_node["progress"] = progress
        self.write(status)

    def record_command(self, node: Dict[str, Any], cmd: List[str], elapsed_ms: int) -> None:
        node["commands"].append({"cmd": cmd, "elapsed_ms": elapsed_ms})

    def write(self, status: str, error: Optional[str] = None) -> Path:
        payload: Dict[str, Any] = {
            "pipeline_id": self.pipeline_id,
            "sample_id": self.sample_id,
            "started_at": self.started_at,
            "finished_at": _now_iso(),
            "status": status,
            "nodes": self.nodes,
        }
        if error:
            payload["error"] = error
            
        # 安全写入：先写临时文件再重命名，防止其它进程读到只写了一半的 JSON
        out = self.outdir / "pipeline_node_records.json"
        tmp_out = out.with_suffix(".json.tmp")
        tmp_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_out.replace(out)
        return out


def _run_with_progress(cmd: List[str], node: Optional[Dict[str, Any]] = None, cwd: Optional[Path] = None, recorder: Optional[Any] = None) -> int:
    t0 = time.time()
    
    # 检查是否是 GATK HaplotypeCaller 这种支持进度输出的命令
    if any("HaplotypeCaller" in c for c in cmd):
        proc = subprocess.Popen(
            cmd,
            stdout=sys.stderr,
            stderr=subprocess.PIPE,
            cwd=str(cwd) if cwd else None,
            text=True,
            bufsize=1,
        )
        assert proc.stderr is not None
        for line in proc.stderr:
            sys.stderr.write(line)
            # GATK typical progress line: 12:34:56.789 INFO  ProgressMeter - ... 5.0% ...
            if "ProgressMeter" in line:
                m = re.search(r"(\d+\.\d+)%", line)
                if m and recorder:
                    prog_val = float(m.group(1))
                    recorder.write_progress("running", int(prog_val))
        proc.wait()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
    else:
        subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, stdout=sys.stderr)
        
    elapsed_ms = int((time.time() - t0) * 1000)
    if node is not None:
        node["commands"].append({"cmd": cmd, "elapsed_ms": elapsed_ms})
    return elapsed_ms

def _run(cmd: List[str], node: Optional[Dict[str, Any]] = None, cwd: Optional[Path] = None) -> int:
    return _run_with_progress(cmd, node, cwd)


def _write_bwa_index_progress(outdir: Path, progress: int) -> None:
    progress_file = outdir / "bwa_index_progress.json"
    payload = {
        "phase": "bwa_index",
        "progress": max(0, min(100, int(progress))),
        "updated_at": _now_iso(),
    }
    progress_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _run_bwa_index_with_progress(bwa_bin: str, ref_fa: Path, outdir: Path, node: Optional[Dict[str, Any]] = None) -> int:
    cmd = [bwa_bin, "index", str(ref_fa)]
    t0 = time.time()
    text_length = 0
    sent_progress = 0
    _write_bwa_index_progress(outdir, 0)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    assert proc.stderr is not None
    stderr_lines = []
    try:
        for line in proc.stderr:
            stderr_lines.append(line)
            if len(stderr_lines) > 50:
                stderr_lines.pop(0)
            m_len = re.search(r"textLength=(\d+)", line)
            if m_len:
                text_length = int(m_len.group(1))
                continue
            m_proc = re.search(r"(\d+)\s+characters processed\.", line)
            if m_proc and text_length > 0:
                processed = int(m_proc.group(1))
                progress = int((processed * 100) / text_length)
                progress = min(99, progress)
                while progress >= sent_progress + 5:
                    sent_progress += 5
                    _write_bwa_index_progress(outdir, sent_progress)
    finally:
        ret = proc.wait()
        if ret != 0:
            err_msg = "".join(stderr_lines)
            raise RuntimeError(f"Command {cmd} returned non-zero exit status {ret}. Stderr: {err_msg}")
    _write_bwa_index_progress(outdir, 100)
    elapsed_ms = int((time.time() - t0) * 1000)
    if node is not None:
        node["commands"].append({"cmd": cmd, "elapsed_ms": elapsed_ms})
    return elapsed_ms


def _require_tool(tool: str) -> None:
    _resolve_tool(tool)


def _resolve_tool(tool: str) -> str:
    resolved = shutil.which(tool)
    if resolved:
        return resolved

    if tool == "gatk":
        candidates = [
            os.getenv("GATK_BIN", "").strip(),
            str(Path.home() / "miniconda3" / "bin" / "gatk"),
            "/Users/lixiuna/miniconda3/bin/gatk",
        ]
        for candidate in candidates:
            if candidate and Path(candidate).exists():
                return candidate

    raise RuntimeError(f"Required tool not found in PATH: {tool}")


def _resolve_ref_input(ref: str) -> Path:
    p = Path(ref)
    if p.exists():
        return p.resolve()
    if ref.endswith(".fa") and Path(f"{ref}.gz").exists():
        return Path(f"{ref}.gz").resolve()
    raise FileNotFoundError(f"Reference file not found: {ref}")


def _materialize_reference(ref_in: Path, outdir: Path) -> Path:
    if ref_in.suffix == ".gz":
        ref_dir = outdir / "ref"
        ref_dir.mkdir(parents=True, exist_ok=True)
        ref_fa = ref_dir / ref_in.stem
        if not ref_fa.exists():
            with gzip.open(ref_in, "rt", encoding="utf-8", errors="replace") as src, ref_fa.open(
                "w", encoding="utf-8"
            ) as dst:
                shutil.copyfileobj(src, dst)
        return ref_fa.resolve()
    if ref_in.suffix in {".fa", ".fasta", ".fna"}:
        return ref_in.resolve()
    raise ValueError(f"Unsupported reference extension: {ref_in}")


def _ensure_reference_indices(ref_fa: Path) -> None:
    bwa_bin = _resolve_tool("bwa")
    samtools_bin = _resolve_tool("samtools")
    gatk_bin = _resolve_tool("gatk")
    if not Path(str(ref_fa) + ".fai").exists():
        _run([samtools_bin, "faidx", str(ref_fa)])
    ref_dict = ref_fa.with_suffix(".dict")
    if not ref_dict.exists():
        _run([gatk_bin, "CreateSequenceDictionary", "-R", str(ref_fa), "-O", str(ref_dict)])
    bwa_idx = [Path(str(ref_fa) + ext) for ext in [".amb", ".ann", ".bwt", ".pac", ".sa"]]
    if not all(p.exists() for p in bwa_idx):
        _run([bwa_bin, "index", str(ref_fa)])


def _inject_reference_header(vcf_path: Path, ref_fa: Path) -> None:
    lines = vcf_path.read_text(encoding="utf-8").splitlines(keepends=True)
    if any(line.startswith("##reference=") for line in lines):
        return
    header = f"##reference={ref_fa}\n"
    insert_at = 0
    for i, line in enumerate(lines):
        if line.startswith("##fileformat="):
            insert_at = i + 1
            break
        if line.startswith("#CHROM"):
            insert_at = i
            break
    lines.insert(insert_at, header)
    vcf_path.write_text("".join(lines), encoding="utf-8")


def _count_vcf_variants(vcf_path: Path) -> int:
    count = 0
    with vcf_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if line and not line.startswith("#"):
                count += 1
    return count


def _count_pass_variants(vcf_path: Path) -> int:
    count = 0
    with vcf_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line or line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 7:
                continue
            filt = parts[6].strip()
            if filt in {"PASS", "."}:
                count += 1
    return count


def _open_text_maybe_gz(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def _fastq_basic_stats(fastq_path: Path) -> Dict[str, Any]:
    reads = 0
    total_bases = 0
    n_bases = 0
    min_len = 0
    max_len = 0
    malformed_lines = 0

    with _open_text_maybe_gz(fastq_path) as fh:
        for i, line in enumerate(fh):
            if i % 4 != 1:
                continue
            seq = line.strip()
            if not seq:
                malformed_lines += 1
                continue
            seq_len = len(seq)
            reads += 1
            total_bases += seq_len
            n_bases += seq.upper().count("N")
            if reads == 1:
                min_len = seq_len
                max_len = seq_len
            else:
                min_len = min(min_len, seq_len)
                max_len = max(max_len, seq_len)

    mean_len = (total_bases / reads) if reads else 0.0
    n_rate = (n_bases / total_bases) if total_bases else 0.0
    return {
        "reads": reads,
        "total_bases": total_bases,
        "n_bases": n_bases,
        "n_rate": round(n_rate, 6),
        "mean_read_length": round(mean_len, 2),
        "min_read_length": min_len,
        "max_read_length": max_len,
        "malformed_lines": malformed_lines,
    }


def _run_raw_qc(
    fastq1: Path,
    fastq2: Optional[Path],
    outdir: Path,
    *,
    qc_gate: bool,
    max_n_rate: float,
) -> Dict[str, Any]:
    qc_dir = outdir / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    r1 = _fastq_basic_stats(fastq1)
    r2 = _fastq_basic_stats(fastq2) if fastq2 else None
    total_bases = r1["total_bases"] + (r2["total_bases"] if r2 else 0)
    total_n_bases = r1["n_bases"] + (r2["n_bases"] if r2 else 0)
    merged_n_rate = (total_n_bases / total_bases) if total_bases else 0.0
    payload = {
        "raw_fastq_qc": {
            "r1": r1,
            "r2": r2,
            "overall": {
                "total_reads": r1["reads"] + (r2["reads"] if r2 else 0),
                "total_bases": total_bases,
                "n_rate": round(merged_n_rate, 6),
                "max_n_rate_threshold": max_n_rate,
            },
        }
    }
    raw_qc_path = qc_dir / "raw_qc.json"
    raw_qc_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if qc_gate and merged_n_rate > max_n_rate:
        raise RuntimeError(
            f"Raw FASTQ QC gate failed: n_rate={merged_n_rate:.6f} > max_n_rate={max_n_rate:.6f}"
        )
    return {
        "raw_qc_path": str(raw_qc_path),
        "r1": r1,
        "r2": r2,
        "overall": payload["raw_fastq_qc"]["overall"],
    }


def _trim_fastq(
    fastp_bin: str,
    fastq1: Path,
    fastq2: Optional[Path],
    outdir: Path,
    *,
    min_read_length: int,
    min_qscore: int,
    threads: int,
    node: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    clean_dir = outdir / "clean"
    qc_dir = outdir / "qc"
    clean_dir.mkdir(parents=True, exist_ok=True)
    qc_dir.mkdir(parents=True, exist_ok=True)

    clean_r1 = clean_dir / "clean_R1.fastq.gz"
    clean_r2 = clean_dir / "clean_R2.fastq.gz"
    fastp_json = qc_dir / "fastp.json"
    fastp_html = qc_dir / "fastp.html"

    cmd = [
        fastp_bin,
        "-i",
        str(fastq1),
        "-o",
        str(clean_r1),
        "-q",
        str(min_qscore),
        "-l",
        str(min_read_length),
        "-w",
        str(max(1, threads)),
        "-j",
        str(fastp_json),
        "-h",
        str(fastp_html),
    ]
    if fastq2:
        cmd.extend(["-I", str(fastq2), "-O", str(clean_r2), "--detect_adapter_for_pe"])
    _run(cmd, node=node)
    return {
        "clean_r1": str(clean_r1),
        "clean_r2": str(clean_r2) if fastq2 else "",
        "fastp_json": str(fastp_json),
        "fastp_html": str(fastp_html),
    }


def _run_post_trim_qc(
    raw_qc: Dict[str, Any],
    clean_r1: Path,
    clean_r2: Optional[Path],
    outdir: Path,
) -> Dict[str, Any]:
    qc_dir = outdir / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    c1 = _fastq_basic_stats(clean_r1)
    c2 = _fastq_basic_stats(clean_r2) if clean_r2 else None

    raw_total_reads = int(raw_qc["overall"]["total_reads"])
    clean_total_reads = c1["reads"] + (c2["reads"] if c2 else 0)
    read_retention = (clean_total_reads / raw_total_reads) if raw_total_reads else 0.0

    raw_total_bases = int(raw_qc["overall"]["total_bases"])
    clean_total_bases = c1["total_bases"] + (c2["total_bases"] if c2 else 0)
    base_retention = (clean_total_bases / raw_total_bases) if raw_total_bases else 0.0

    clean_n_bases = c1["n_bases"] + (c2["n_bases"] if c2 else 0)
    clean_n_rate = (clean_n_bases / clean_total_bases) if clean_total_bases else 0.0
    raw_n_rate = float(raw_qc["overall"]["n_rate"])

    payload = {
        "post_trim_qc": {
            "r1": c1,
            "r2": c2,
            "overall": {
                "total_reads": clean_total_reads,
                "total_bases": clean_total_bases,
                "n_rate": round(clean_n_rate, 6),
                "raw_n_rate": raw_n_rate,
                "n_rate_delta": round(raw_n_rate - clean_n_rate, 6),
                "read_retention": round(read_retention, 6),
                "base_retention": round(base_retention, 6),
            },
        }
    }
    post_qc_path = qc_dir / "post_trim_qc.json"
    post_qc_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "post_qc_path": str(post_qc_path),
        "overall": payload["post_trim_qc"]["overall"],
    }


def _mark_duplicates(
    gatk_bin: str,
    sorted_bam: Path,
    sample: str,
    outdir: Path,
    node: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    dedup_bam = outdir / f"{sample}.dedup.bam"
    metrics = outdir / f"{sample}.dedup.metrics.txt"
    cmd = [
        gatk_bin,
        "MarkDuplicates",
        "-I",
        str(sorted_bam),
        "-O",
        str(dedup_bam),
        "-M",
        str(metrics),
        "--CREATE_INDEX",
        "true",
    ]
    _run(cmd, node=node)
    return {
        "dedup_bam": str(dedup_bam),
        "dedup_bai": str(Path(str(dedup_bam) + ".bai")),
        "dedup_metrics": str(metrics),
    }


def _bam_qc(samtools_bin: str, bam: Path, outdir: Path, node: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    qc_dir = outdir / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    
    flagstat_txt = qc_dir / "bam_flagstat.txt"
    cmd_flagstat = [samtools_bin, "flagstat", str(bam)]
    t0 = time.time()
    with flagstat_txt.open("w") as f:
        subprocess.run(cmd_flagstat, stdout=f, check=True)
    if node is not None:
        node["commands"].append({"cmd": cmd_flagstat, "elapsed_ms": int((time.time() - t0) * 1000)})
        
    stats_txt = qc_dir / "bam_stats.txt"
    cmd_stats = [samtools_bin, "stats", str(bam)]
    t0 = time.time()
    with stats_txt.open("w") as f:
        subprocess.run(cmd_stats, stdout=f, check=True)
    if node is not None:
        node["commands"].append({"cmd": cmd_stats, "elapsed_ms": int((time.time() - t0) * 1000)})
        
    coverage_txt = qc_dir / "bam_coverage.txt"
    cmd_cov = [samtools_bin, "coverage", str(bam)]
    t0 = time.time()
    with coverage_txt.open("w") as f:
        subprocess.run(cmd_cov, stdout=f, check=True)
    if node is not None:
        node["commands"].append({"cmd": cmd_cov, "elapsed_ms": int((time.time() - t0) * 1000)})
        
    reads_mapped = 0
    reads_total = 0
    reads_duplicated = 0
    with stats_txt.open("r") as f:
        for line in f:
            if line.startswith("SN\t"):
                parts = line.split("\t")
                if parts[1] == "raw total sequences:":
                    reads_total = int(parts[2])
                elif parts[1] == "reads mapped:":
                    reads_mapped = int(parts[2])
                elif parts[1] == "reads duplicated:":
                    reads_duplicated = int(parts[2])
                    
    mapping_rate = (reads_mapped / reads_total) if reads_total > 0 else 0.0
    duplicate_rate = (reads_duplicated / reads_total) if reads_total > 0 else 0.0
        
    total_covbases = 0
    total_len = 0
    sum_depth_len = 0.0
    with coverage_txt.open("r") as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 7:
                length = int(parts[2]) - int(parts[1]) + 1
                covbases = int(parts[4])
                depth = float(parts[6])
                total_covbases += covbases
                total_len += length
                sum_depth_len += depth * length
                
    mean_depth = (sum_depth_len / total_len) if total_len > 0 else 0.0
    coverage = (total_covbases / total_len) if total_len > 0 else 0.0
        
    stats = {
        "mapping_rate": round(mapping_rate, 4),
        "duplicate_rate": round(duplicate_rate, 4),
        "mean_depth": round(mean_depth, 4),
        "coverage": round(coverage, 4)
    }
    
    qc_json = qc_dir / "alignment_qc.json"
    qc_json.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    
    return {
        "alignment_qc_json": str(qc_json),
        "stats": stats
    }


def _run_bqsr(gatk_bin: str, bam: Path, ref_fa: Path, known_sites: Path, sample: str, outdir: Path, node: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    recal_table = outdir / f"{sample}.recal.table"
    recal_bam = outdir / f"{sample}.recal.bam"
    
    cmd1 = [
        gatk_bin, "BaseRecalibrator",
        "-I", str(bam),
        "-R", str(ref_fa),
        "--known-sites", str(known_sites),
        "-O", str(recal_table)
    ]
    _run(cmd1, node=node)
    
    cmd2 = [
        gatk_bin, "ApplyBQSR",
        "-I", str(bam),
        "-R", str(ref_fa),
        "--bqsr-recal-file", str(recal_table),
        "-O", str(recal_bam)
    ]
    _run(cmd2, node=node)
    
    return {
        "recal_bam": str(recal_bam),
        "recal_table": str(recal_table)
    }



def _filter_variants_hard(
    gatk_bin: str,
    raw_vcf: Path,
    sample: str,
    outdir: Path,
    node: Optional[Dict[str, Any]] = None,
) -> Path:
    """
    使用 GATK 推荐的单样本硬过滤标准进行变异过滤。
    针对 SNP 的典型硬过滤指标：
    QD < 2.0 || FS > 60.0 || MQ < 40.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0 || QUAL < 30.0 || DP < 10
    """
    filtered_vcf = outdir / f"{sample}.variants.filtered.vcf"
    cmd = [
        gatk_bin,
        "VariantFiltration",
        "-V",
        str(raw_vcf),
        "-O",
        str(filtered_vcf),
        "--filter-name", "LOW_QUAL",
        "--filter-expression", "QUAL < 30.0",
        "--filter-name", "LOW_DP",
        "--filter-expression", "DP < 10",
        "--filter-name", "LOW_QD",
        "--filter-expression", "QD < 2.0",
        "--filter-name", "HIGH_FS",
        "--filter-expression", "FS > 60.0",
        "--filter-name", "LOW_MQ",
        "--filter-expression", "MQ < 40.0",
        "--filter-name", "LOW_MQRankSum",
        "--filter-expression", "MQRankSum < -12.5",
        "--filter-name", "LOW_ReadPosRankSum",
        "--filter-expression", "ReadPosRankSum < -8.0",
    ]
    _run(cmd, node=node)
    return filtered_vcf


def _parse_info(info: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not info or info == ".":
        return out
    for token in info.split(";"):
        if "=" in token:
            k, v = token.split("=", 1)
            out[k] = v
    return out


def _pick_snp_name(record_id: str, info_map: Dict[str, str], chrom: str, pos: str, ref: str, alt: str) -> str:
    rid = (record_id or "").strip()
    if rid and rid != ".":
        if rid.lower().startswith("rs"):
            return rid
        if rid.isdigit():
            return f"rs{rid}"

    for key in ("RS", "RSID", "dbSNP", "DBSNP"):
        v = (info_map.get(key) or "").strip()
        if not v:
            continue
        if v.lower().startswith("rs"):
            return v
        if v.isdigit():
            return f"rs{v}"

    return f"{chrom}:{pos}:{ref}>{alt}"


def _vcf_to_csv(vcf_path: Path, csv_path: Path) -> Dict[str, Any]:
    import csv

    rows: List[Dict[str, Any]] = []
    with vcf_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line or line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 8:
                continue
            chrom, pos, record_id, ref, alt, qual, _, info = parts[:8]
            info_map = _parse_info(info)
            dp = info_map.get("DP", "")
            af = info_map.get("AF", "")
            rows.append(
                {
                    "snp_name": _pick_snp_name(record_id, info_map, chrom, pos, ref, alt),
                    "chrom": chrom,
                    "pos": int(pos),
                    "ref": ref,
                    "alt": alt,
                    "qual": float(qual) if qual not in {".", ""} else 0.0,
                    "dp": int(dp) if dp.isdigit() else 0,
                    "af": float(af) if af else 0.0,
                }
            )

    with csv_path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out,
            fieldnames=["snp_name", "chrom", "pos", "ref", "alt", "qual", "dp", "af", "risk_tag"],
        )
        writer.writeheader()
        for r in rows:
            risk_tag = "high" if r["af"] >= 0.3 or r["qual"] >= 100 else "moderate"
            writer.writerow(
                {
                    **r,
                    "risk_tag": risk_tag,
                }
            )
    return {"rows": len(rows)}


def _predict_disease(csv_path: Path) -> Dict[str, Any]:
    import csv

    variant_count = 0
    high_risk_count = 0
    af_sum = 0.0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            variant_count += 1
            af = float(row.get("af") or 0.0)
            af_sum += af
            if (row.get("risk_tag") or "").lower() == "high":
                high_risk_count += 1

    mean_af = (af_sum / variant_count) if variant_count else 0.0
    base = min(0.95, 0.15 + 0.55 * mean_af + 0.3 * (high_risk_count / max(1, variant_count)))
    predictions = [
        {"disease": "肿瘤相关遗传风险", "score": round(base, 3)},
        {"disease": "心血管代谢风险", "score": round(min(0.95, base * 0.85 + 0.05), 3)},
        {"disease": "神经退行性风险", "score": round(min(0.95, base * 0.75 + 0.08), 3)},
    ]
    level = "low"
    if base >= 0.7:
        level = "high"
    elif base >= 0.4:
        level = "moderate"
    return {
        "overall_risk_level": level,
        "overall_score": round(base, 3),
        "variant_count": variant_count,
        "high_risk_variant_count": high_risk_count,
        "mean_af": round(mean_af, 4),
        "predictions": predictions,
    }


def _write_report(
    report_path: Path,
    fastq1: Path,
    fastq2: Optional[Path],
    ref_fa: Path,
    vcf_path: Path,
    csv_path: Path,
    prediction: Dict[str, Any],
    bam_path: Path,
    variants: int,
) -> None:
    lines = [
        "# Disease Association Report",
        "",
        "## Inputs",
        f"- FASTQ R1: `{fastq1}`",
        f"- FASTQ R2: `{fastq2}`" if fastq2 else "- FASTQ R2: `N/A (single-end)`",
        f"- Reference: `{ref_fa}`",
        "",
        "## Pipeline",
        "- FASTQ QC + trimming: fastp",
        "- Alignment: BWA-MEM",
        "- BAM processing: samtools sort/index + GATK MarkDuplicates",
        "- Variant calling: GATK HaplotypeCaller",
        "- Variant filtering: GATK VariantFiltration (hard-filter)",
        "",
        "## Outputs",
        f"- BAM: `{bam_path}`",
        f"- VCF: `{vcf_path}`",
        f"- CSV: `{csv_path}`",
        f"- Variants: `{variants}`",
        "",
        "## Disease Prediction",
        f"- Overall risk level: `{prediction['overall_risk_level']}`",
        f"- Overall score: `{prediction['overall_score']}`",
        f"- Mean AF: `{prediction['mean_af']}`",
        f"- High-risk variants: `{prediction['high_risk_variant_count']}` / `{prediction['variant_count']}`",
        "",
        "## Top Predicted Risks",
    ]
    for item in prediction["predictions"]:
        lines.append(f"- {item['disease']}: score `{item['score']}`")
    lines += [
        "",
        "## Node Records",
        "- Full node-level records are written to `pipeline_node_records.json` in the output directory.",
        "",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    _ensure_runtime_path()
    parser = argparse.ArgumentParser(description="Production FASTQ -> VCF pipeline (BWA/GATK).")
    parser.add_argument("--fastq", required=False, help="FASTQ R1 path (can be passed via env FASTQ)")
    parser.add_argument("--fastq2", default=None, help="FASTQ R2 path (optional)")
    parser.add_argument("--ref", required=False, help="Reference genome FASTA(.gz) path (can be passed via env REF)")
    parser.add_argument("--outdir", required=False, help="Output directory (can be passed via env OUTDIR)")
    parser.add_argument("--sample-id", default="sample1", help="Sample ID")
    parser.add_argument("--threads", type=int, default=8, help="Thread count")
    parser.add_argument("--min-read-length", type=int, default=50, help="Minimum read length after trimming")
    parser.add_argument("--min-qscore", type=int, default=20, help="Minimum quality threshold for trimming")
    parser.add_argument("--max-n-rate", type=float, default=0.1, help="Maximum allowed N base ratio in FASTQ")
    parser.add_argument("--qc-gate", action="store_true", help="Fail pipeline when QC threshold is not met")
    parser.add_argument("--run-bqsr", action="store_true", help="Run BQSR (requires --known-sites)")
    parser.add_argument("--known-sites", required=False, help="VCF file of known sites for BQSR")
    args = parser.parse_args()

    fastq_path = args.fastq or os.environ.get("FASTQ")
    if not fastq_path:
        parser.error("--fastq or FASTQ env var is required")
        
    ref_path = args.ref or os.environ.get("REF")
    if not ref_path:
        parser.error("--ref or REF env var is required")
        
    outdir_path = args.outdir or os.environ.get("OUTDIR")
    if not outdir_path:
        parser.error("--outdir or OUTDIR env var is required")

    fastq1 = Path(fastq_path).resolve()
    fastq2 = Path(args.fastq2).resolve() if args.fastq2 else None
    outdir = Path(outdir_path).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    recorder = NodeRecorder(
        pipeline_id=f"bio-{uuid.uuid4().hex[:10]}",
        sample_id=args.sample_id,
        outdir=outdir,
    )

    try:
        n_tools = recorder.start("tool_check", {"tools": ["bwa", "samtools", "gatk", "fastp", "bcftools"]})
        bwa_bin = _resolve_tool("bwa")
        samtools_bin = _resolve_tool("samtools")
        gatk_bin = _resolve_tool("gatk")
        fastp_bin = _resolve_tool("fastp")
        _resolve_tool("bcftools")
        recorder.finish(n_tools)

        n_ref_resolve = recorder.start("resolve_reference_input", {"ref_arg": args.ref})
        ref_in = _resolve_ref_input(args.ref)
        recorder.finish(n_ref_resolve, outputs={"ref_input": str(ref_in)})

        n_ref_mat = recorder.start("materialize_reference", {"ref_input": str(ref_in)})
        ref_fa = _materialize_reference(ref_in, outdir)
        recorder.finish(n_ref_mat, outputs={"ref_fasta": str(ref_fa)})

        n_ref_idx = recorder.start("build_reference_index", {"ref_fasta": str(ref_fa)})
        if not Path(str(ref_fa) + ".fai").exists():
            _run([samtools_bin, "faidx", str(ref_fa)], node=n_ref_idx)
        ref_dict = ref_fa.with_suffix(".dict")
        if not ref_dict.exists():
            _run([gatk_bin, "CreateSequenceDictionary", "-R", str(ref_fa), "-O", str(ref_dict)], node=n_ref_idx)
        bwa_idx = [Path(str(ref_fa) + ext) for ext in [".amb", ".ann", ".bwt", ".pac", ".sa"]]
        if not all(p.exists() for p in bwa_idx):
            _run_bwa_index_with_progress(bwa_bin, ref_fa, outdir, node=n_ref_idx)
        recorder.finish(
            n_ref_idx,
            outputs={
                "fai": str(Path(str(ref_fa) + ".fai")),
                "dict": str(ref_dict),
            },
        )

        sample = args.sample_id
        sam = outdir / f"{sample}.sam"
        sorted_bam = outdir / f"{sample}.sorted.bam"
        dedup_bam = outdir / f"{sample}.dedup.bam"
        raw_vcf = outdir / f"{sample}.variants.raw.vcf"
        filtered_vcf = outdir / f"{sample}.variants.filtered.vcf"
        csv_file = outdir / "mutations.csv"
        report = outdir / "disease_association_report.md"
        prediction_json = outdir / "disease_prediction.json"

        n_raw_qc = recorder.start(
            "raw_fastq_qc",
            {
                "fastq1": str(fastq1),
                "fastq2": str(fastq2) if fastq2 else None,
                "qc_gate": args.qc_gate,
                "max_n_rate": args.max_n_rate,
            },
        )
        raw_qc = _run_raw_qc(
            fastq1,
            fastq2,
            outdir,
            qc_gate=args.qc_gate,
            max_n_rate=args.max_n_rate,
        )
        recorder.finish(
            n_raw_qc,
            outputs={"raw_qc_json": raw_qc["raw_qc_path"]},
            stats={
                "total_reads": raw_qc["overall"]["total_reads"],
                "n_rate": raw_qc["overall"]["n_rate"],
            },
        )

        n_trim = recorder.start(
            "trim_adapters_and_low_quality",
            {
                "fastq1": str(fastq1),
                "fastq2": str(fastq2) if fastq2 else None,
                "min_read_length": args.min_read_length,
                "min_qscore": args.min_qscore,
            },
        )
        trim_outputs = _trim_fastq(
            fastp_bin,
            fastq1,
            fastq2,
            outdir,
            min_read_length=args.min_read_length,
            min_qscore=args.min_qscore,
            threads=args.threads,
            node=n_trim,
        )
        clean_fastq1 = Path(trim_outputs["clean_r1"]).resolve()
        clean_fastq2 = Path(trim_outputs["clean_r2"]).resolve() if trim_outputs["clean_r2"] else None
        recorder.finish(
            n_trim,
            outputs={
                "clean_r1": str(clean_fastq1),
                "clean_r2": str(clean_fastq2) if clean_fastq2 else "",
                "fastp_json": trim_outputs["fastp_json"],
                "fastp_html": trim_outputs["fastp_html"],
            },
        )

        n_post_trim_qc = recorder.start(
            "post_trim_qc",
            {
                "clean_r1": str(clean_fastq1),
                "clean_r2": str(clean_fastq2) if clean_fastq2 else None,
            },
        )
        post_trim_qc = _run_post_trim_qc(raw_qc, clean_fastq1, clean_fastq2, outdir)
        recorder.finish(
            n_post_trim_qc,
            outputs={"post_trim_qc_json": post_trim_qc["post_qc_path"]},
            stats={
                "read_retention": post_trim_qc["overall"]["read_retention"],
                "n_rate_delta": post_trim_qc["overall"]["n_rate_delta"],
            },
        )

        rg = f"@RG\\tID:{sample}\\tSM:{sample}\\tPL:ILLUMINA"
        bwa_cmd = [bwa_bin, "mem", "-t", str(args.threads), "-R", rg, str(ref_fa), str(clean_fastq1)]
        if clean_fastq2:
            bwa_cmd.append(str(clean_fastq2))

        n_align = recorder.start(
            "align_reads_bwa_mem",
            {
                "fastq1": str(clean_fastq1),
                "fastq2": str(clean_fastq2) if clean_fastq2 else None,
                "threads": args.threads,
            },
        )
        with sam.open("w", encoding="utf-8") as sam_out:
            t0 = time.time()
            subprocess.run(bwa_cmd, stdout=sam_out, check=True)
            recorder.record_command(n_align, bwa_cmd, int((time.time() - t0) * 1000))
        recorder.finish(n_align, outputs={"sam": str(sam)})

        n_sort = recorder.start("sort_bam_samtools", {"sam": str(sam)})
        _run([samtools_bin, "sort", "-@", str(args.threads), "-o", str(sorted_bam), str(sam)], node=n_sort)
        recorder.finish(n_sort, outputs={"bam": str(sorted_bam)})

        n_dedup = recorder.start("mark_duplicates", {"sorted_bam": str(sorted_bam)})
        dedup_outputs = _mark_duplicates(gatk_bin, sorted_bam, sample, outdir, node=n_dedup)
        dedup_bam = Path(dedup_outputs["dedup_bam"]).resolve()
        recorder.finish(
            n_dedup,
            outputs={
                "dedup_bam": str(dedup_bam),
                "dedup_bai": dedup_outputs["dedup_bai"],
                "dedup_metrics": dedup_outputs["dedup_metrics"],
            },
        )

        n_bam_idx = recorder.start("index_bam_samtools", {"bam": str(dedup_bam)})
        _run([samtools_bin, "index", str(dedup_bam)], node=n_bam_idx)
        recorder.finish(n_bam_idx, outputs={"bai": str(Path(str(dedup_bam) + ".bai"))})

        n_align_qc = recorder.start("alignment_qc", {"bam": str(dedup_bam)})
        align_qc_res = _bam_qc(samtools_bin, dedup_bam, outdir, node=n_align_qc)
        recorder.finish(n_align_qc, outputs={"alignment_qc_json": align_qc_res["alignment_qc_json"]}, stats=align_qc_res["stats"])

        if args.run_bqsr:
            if not args.known_sites:
                raise RuntimeError("--run-bqsr requires --known-sites")
            known_sites_path = Path(args.known_sites).resolve()
            n_bqsr = recorder.start("bqsr_recalibration", {"bam": str(dedup_bam), "known_sites": str(known_sites_path)})
            bqsr_res = _run_bqsr(gatk_bin, dedup_bam, ref_fa, known_sites_path, sample, outdir, node=n_bqsr)
            dedup_bam = Path(bqsr_res["recal_bam"]).resolve()
            recorder.finish(n_bqsr, outputs={"recal_bam": str(dedup_bam), "recal_table": bqsr_res["recal_table"]})
            
            n_recal_idx = recorder.start("index_recal_bam", {"bam": str(dedup_bam)})
            _run([samtools_bin, "index", str(dedup_bam)], node=n_recal_idx)
            recorder.finish(n_recal_idx, outputs={"bai": str(Path(str(dedup_bam) + ".bai"))})

        n_call = recorder.start("call_variants_gatk_haplotypecaller", {"bam": str(dedup_bam), "ref": str(ref_fa)})
        _run_with_progress(
            [
                gatk_bin,
                "HaplotypeCaller",
                "-R",
                str(ref_fa),
                "-I",
                str(dedup_bam),
                "-O",
                str(raw_vcf),
            ],
            node=n_call,
            recorder=recorder
        )
        recorder.finish(n_call, outputs={"vcf": str(raw_vcf)})

        n_vcf_header = recorder.start("ensure_vcf_reference_header", {"vcf": str(raw_vcf), "ref": str(ref_fa)})
        _inject_reference_header(raw_vcf, ref_fa)
        raw_variants = _count_vcf_variants(raw_vcf)
        recorder.finish(n_vcf_header, stats={"variants": raw_variants})

        n_filter = recorder.start("filter_variants_hard", {"vcf": str(raw_vcf)})
        filtered_vcf = _filter_variants_hard(gatk_bin, raw_vcf, sample, outdir, node=n_filter)
        variants = _count_pass_variants(filtered_vcf)
        filtered_total = _count_vcf_variants(filtered_vcf)
        filter_stats = {
            "raw_variants": raw_variants,
            "filtered_variants": filtered_total,
            "pass_variants": variants,
        }
        recorder.finish(
            n_filter,
            outputs={"filtered_vcf": str(filtered_vcf)},
            stats=filter_stats,
        )

        n_vcf_header_filtered = recorder.start(
            "ensure_filtered_vcf_reference_header", {"vcf": str(filtered_vcf), "ref": str(ref_fa)}
        )
        _inject_reference_header(filtered_vcf, ref_fa)
        recorder.finish(n_vcf_header_filtered, stats={"pass_variants": variants})

        n_csv = recorder.start("convert_vcf_to_csv", {"vcf": str(filtered_vcf)})
        csv_stats = _vcf_to_csv(filtered_vcf, csv_file)
        recorder.finish(n_csv, outputs={"csv": str(csv_file)}, stats=csv_stats)

        n_pred = recorder.start("disease_prediction_from_csv", {"csv": str(csv_file)})
        prediction = _predict_disease(csv_file)
        prediction_json.write_text(json.dumps(prediction, ensure_ascii=False, indent=2), encoding="utf-8")
        recorder.finish(
            n_pred,
            outputs={"prediction_json": str(prediction_json)},
            stats={
                "overall_risk_level": prediction["overall_risk_level"],
                "variant_count": prediction["variant_count"],
            },
        )

        n_report = recorder.start("generate_markdown_report", {"report": str(report)})
        _write_report(report, fastq1, fastq2, ref_fa, filtered_vcf, csv_file, prediction, dedup_bam, variants)
        recorder.finish(n_report, outputs={"report": str(report)})

        n_cleanup = recorder.start("cleanup_temp_files", {"sam": str(sam)})
        sam.unlink(missing_ok=True)
        recorder.finish(n_cleanup, outputs={"sam_removed": str(sam)})

        records_path = recorder.write("ok")
        print(
            json.dumps(
                {
                    "ok": True,
                    "vcf": str(filtered_vcf),
                    "raw_vcf": str(raw_vcf),
                    "csv": str(csv_file),
                    "report": str(report),
                    "prediction_json": str(prediction_json),
                    "bam": str(dedup_bam),
                    "reference": str(ref_fa),
                    "variants": variants,
                    "node_records": str(records_path),
                },
                ensure_ascii=False,
            )
        )
        return 0
    except Exception as e:
        records_path = recorder.write("failed", error=str(e))
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": str(e),
                    "node_records": str(records_path),
                },
                ensure_ascii=False,
            )
        )
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
