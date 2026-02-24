#!/usr/bin/env python3
"""
External entrypoint for launching Lobster multi-agent pipelines.

Examples:
  python run_pipeline.py --input-type fastq --fastq sample_R1.fastq sample_R2.fastq
  python run_pipeline.py --input-type vcf --vcf sample.vcf.gz --json
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional


def _resolve_lobster_bin(user_bin: Optional[str]) -> str:
    """Resolve lobster executable path."""
    if user_bin:
        p = Path(user_bin).expanduser()
        if p.exists():
            return str(p)
        raise FileNotFoundError(f"Specified lobster binary not found: {p}")

    env_bin = os.getenv("LOBSTER_BIN")
    if env_bin:
        p = Path(env_bin).expanduser()
        if p.exists():
            return str(p)
        raise FileNotFoundError(f"LOBSTER_BIN points to missing file: {p}")

    local_bin = Path(__file__).resolve().parent / "lobster" / ".venv" / "bin" / "lobster"
    if local_bin.exists():
        return str(local_bin)

    path_bin = shutil.which("lobster")
    if path_bin:
        return path_bin

    raise FileNotFoundError(
        "Cannot find lobster executable. "
        "Set --lobster-bin, set LOBSTER_BIN, or install lobster to PATH."
    )


def _validate_paths(paths: List[str], label: str) -> List[Path]:
    """Validate user-supplied input paths."""
    out: List[Path] = []
    for raw in paths:
        p = Path(raw).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"{label} path does not exist: {p}")
        out.append(p)
    return out


def _build_prompt(
    input_type: str,
    fastq_paths: List[Path],
    vcf_path: Optional[Path],
    output_dir: Path,
    extra_instruction: str,
) -> str:
    """Build a single natural-language request for Lobster multi-agent workflow."""
    output_dir_txt = str(output_dir)
    if input_type == "fastq":
        fastq_txt = ", ".join(str(p) for p in fastq_paths)
        base = (
            "请执行多智能体基因组分析流程，并在每步完成后继续下一步。"
            f"输入 FASTQ 文件: {fastq_txt}。"
            f"目标输出目录: {output_dir_txt}。"
            "流程要求："
            "1) 从 FASTQ 开始进行变异分析并生成 VCF；"
            "2) 基于 VCF 做变异注释；"
            "3) 导出 mutations.csv；"
            "4) 生成疾病关联报告（Markdown）。"
            "请在结束时汇总关键统计并给出所有输出文件路径。"
        )
    else:
        assert vcf_path is not None
        base = (
            "请执行多智能体基因组分析流程，并在每步完成后继续下一步。"
            f"输入 VCF 文件: {vcf_path}。"
            f"目标输出目录: {output_dir_txt}。"
            "流程要求："
            "1) 对 VCF 做变异注释；"
            "2) 导出 mutations.csv；"
            "3) 生成疾病关联报告（Markdown）。"
            "请在结束时汇总关键统计并给出所有输出文件路径。"
        )

    if extra_instruction.strip():
        base += f" 额外要求：{extra_instruction.strip()}"
    return base


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch Lobster multi-agent workflow from external script."
    )
    parser.add_argument(
        "--lobster-bin",
        default=None,
        help="Path to lobster executable. If omitted, auto-discover.",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="Lobster workspace path. Default: ./lobster_runs/<timestamp>",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Session ID for continuity. Default: ext_pipeline_<timestamp>",
    )
    parser.add_argument(
        "--input-type",
        choices=["auto", "fastq", "vcf"],
        default="auto",
        help="Input type. auto decides from provided arguments.",
    )
    parser.add_argument(
        "--fastq",
        nargs="*",
        default=[],
        help="FASTQ input files (space-separated).",
    )
    parser.add_argument(
        "--vcf",
        default=None,
        help="VCF input file path.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Target output directory to mention in query (created if missing).",
    )
    parser.add_argument(
        "--extra-instruction",
        default="",
        help="Extra natural-language instruction appended to query.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Use `lobster query --json` mode.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print resolved command and query without executing.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = Path(__file__).resolve().parent

    workspace = Path(args.workspace).expanduser().resolve() if args.workspace else (root / "lobster_runs" / ts)
    workspace.mkdir(parents=True, exist_ok=True)

    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (workspace / "outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    session_id = args.session_id or f"ext_pipeline_{ts}"

    fastq_paths = _validate_paths(args.fastq, "FASTQ") if args.fastq else []
    vcf_path = Path(args.vcf).expanduser().resolve() if args.vcf else None
    if vcf_path and not vcf_path.exists():
        raise FileNotFoundError(f"VCF path does not exist: {vcf_path}")

    input_type = args.input_type
    if input_type == "auto":
        if fastq_paths:
            input_type = "fastq"
        elif vcf_path:
            input_type = "vcf"
        else:
            raise ValueError("No input provided. Pass --fastq ... or --vcf ...")

    if input_type == "fastq" and not fastq_paths:
        raise ValueError("--input-type fastq requires --fastq")
    if input_type == "vcf" and not vcf_path:
        raise ValueError("--input-type vcf requires --vcf")

    lobster_bin = _resolve_lobster_bin(args.lobster_bin)
    prompt = _build_prompt(
        input_type=input_type,
        fastq_paths=fastq_paths,
        vcf_path=vcf_path,
        output_dir=output_dir,
        extra_instruction=args.extra_instruction,
    )

    cmd = [
        lobster_bin,
        "query",
        "--workspace",
        str(workspace),
        "--session-id",
        session_id,
    ]
    if args.json:
        cmd.append("--json")
    cmd.append(prompt)

    print("[run_pipeline] input_type:", input_type)
    print("[run_pipeline] workspace :", workspace)
    print("[run_pipeline] output_dir:", output_dir)
    print("[run_pipeline] session_id:", session_id)
    print("[run_pipeline] command   :", " ".join(shlex.quote(part) for part in cmd))

    if args.dry_run:
        print("[run_pipeline] dry-run enabled, command not executed.")
        return 0

    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except KeyboardInterrupt:
        print("\n[run_pipeline] interrupted by user.")
        return 130


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"[run_pipeline] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
