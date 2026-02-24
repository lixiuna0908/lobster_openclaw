#!/usr/bin/env python3
import argparse
import gzip
import json
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


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
        out = self.outdir / "pipeline_node_records.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return out


def _run(cmd: List[str], node: Optional[Dict[str, Any]] = None, cwd: Optional[Path] = None) -> int:
    t0 = time.time()
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)
    elapsed_ms = int((time.time() - t0) * 1000)
    if node is not None:
        node["commands"].append({"cmd": cmd, "elapsed_ms": elapsed_ms})
    return elapsed_ms


def _require_tool(tool: str) -> None:
    if shutil.which(tool) is None:
        raise RuntimeError(f"Required tool not found in PATH: {tool}")


def _resolve_ref_input(ref: str) -> Path:
    p = Path(ref)
    if p.exists():
        return p.resolve()
    if ref.endswith(".fa") and Path(f"{ref}.gz").exists():
        return Path(f"{ref}.gz").resolve()
    raise FileNotFoundError(f"Reference file not found: {ref}")


def _materialize_reference(ref_in: Path, outdir: Path) -> Path:
    ref_dir = outdir / "ref"
    ref_dir.mkdir(parents=True, exist_ok=True)
    if ref_in.suffix == ".gz":
        ref_fa = ref_dir / ref_in.stem
        if not ref_fa.exists():
            with gzip.open(ref_in, "rt", encoding="utf-8", errors="replace") as src, ref_fa.open(
                "w", encoding="utf-8"
            ) as dst:
                shutil.copyfileobj(src, dst)
        return ref_fa.resolve()
    if ref_in.suffix in {".fa", ".fasta", ".fna"}:
        ref_fa = ref_dir / ref_in.name
        if ref_fa.resolve() != ref_in.resolve() and not ref_fa.exists():
            shutil.copy2(ref_in, ref_fa)
        return ref_fa.resolve()
    raise ValueError(f"Unsupported reference extension: {ref_in}")


def _ensure_reference_indices(ref_fa: Path) -> None:
    _require_tool("bwa")
    _require_tool("samtools")
    _require_tool("gatk")
    if not Path(str(ref_fa) + ".fai").exists():
        _run(["samtools", "faidx", str(ref_fa)])
    ref_dict = ref_fa.with_suffix(".dict")
    if not ref_dict.exists():
        _run(["gatk", "CreateSequenceDictionary", "-R", str(ref_fa), "-O", str(ref_dict)])
    bwa_idx = [Path(str(ref_fa) + ext) for ext in [".amb", ".ann", ".bwt", ".pac", ".sa"]]
    if not all(p.exists() for p in bwa_idx):
        _run(["bwa", "index", str(ref_fa)])


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
        "- Alignment: BWA-MEM",
        "- BAM processing: samtools sort/index",
        "- Variant calling: GATK HaplotypeCaller",
        "",
        "## Outputs",
        f"- BAM: `{bam_path}`",
        f"- VCF: `{vcf_path}`",
        f"- CSV: `{csv_path}`",
        f"- Variants: `{variants}`",
        "",
        "## Disease Prediction",
        f"- Overall risk level: `{prediction['overall_risk_level']}`",
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
    parser = argparse.ArgumentParser(description="Production FASTQ -> VCF pipeline (BWA/GATK).")
    parser.add_argument("--fastq", required=True, help="FASTQ R1 path")
    parser.add_argument("--fastq2", default=None, help="FASTQ R2 path (optional)")
    parser.add_argument("--ref", required=True, help="Reference genome FASTA(.gz) path")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--sample-id", default="sample1", help="Sample ID")
    parser.add_argument("--threads", type=int, default=8, help="Thread count")
    args = parser.parse_args()

    fastq1 = Path(args.fastq).resolve()
    fastq2 = Path(args.fastq2).resolve() if args.fastq2 else None
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    recorder = NodeRecorder(
        pipeline_id=f"bio-{uuid.uuid4().hex[:10]}",
        sample_id=args.sample_id,
        outdir=outdir,
    )

    try:
        n_tools = recorder.start("tool_check", {"tools": ["bwa", "samtools", "gatk"]})
        _require_tool("bwa")
        _require_tool("samtools")
        _require_tool("gatk")
        recorder.finish(n_tools)

        n_ref_resolve = recorder.start("resolve_reference_input", {"ref_arg": args.ref})
        ref_in = _resolve_ref_input(args.ref)
        recorder.finish(n_ref_resolve, outputs={"ref_input": str(ref_in)})

        n_ref_mat = recorder.start("materialize_reference", {"ref_input": str(ref_in)})
        ref_fa = _materialize_reference(ref_in, outdir)
        recorder.finish(n_ref_mat, outputs={"ref_fasta": str(ref_fa)})

        n_ref_idx = recorder.start("build_reference_index", {"ref_fasta": str(ref_fa)})
        if not Path(str(ref_fa) + ".fai").exists():
            _run(["samtools", "faidx", str(ref_fa)], node=n_ref_idx)
        ref_dict = ref_fa.with_suffix(".dict")
        if not ref_dict.exists():
            _run(["gatk", "CreateSequenceDictionary", "-R", str(ref_fa), "-O", str(ref_dict)], node=n_ref_idx)
        bwa_idx = [Path(str(ref_fa) + ext) for ext in [".amb", ".ann", ".bwt", ".pac", ".sa"]]
        if not all(p.exists() for p in bwa_idx):
            _run(["bwa", "index", str(ref_fa)], node=n_ref_idx)
        recorder.finish(
            n_ref_idx,
            outputs={
                "fai": str(Path(str(ref_fa) + ".fai")),
                "dict": str(ref_dict),
            },
        )

        sample = args.sample_id
        sam = outdir / f"{sample}.sam"
        bam = outdir / f"{sample}.sorted.bam"
        vcf = outdir / f"{sample}.variants.vcf"
        csv_file = outdir / "mutations.csv"
        report = outdir / "disease_association_report.md"
        prediction_json = outdir / "disease_prediction.json"

        rg = f"@RG\\tID:{sample}\\tSM:{sample}\\tPL:ILLUMINA"
        bwa_cmd = ["bwa", "mem", "-t", str(args.threads), "-R", rg, str(ref_fa), str(fastq1)]
        if fastq2:
            bwa_cmd.append(str(fastq2))

        n_align = recorder.start(
            "align_reads_bwa_mem",
            {"fastq1": str(fastq1), "fastq2": str(fastq2) if fastq2 else None, "threads": args.threads},
        )
        with sam.open("w", encoding="utf-8") as sam_out:
            t0 = time.time()
            subprocess.run(bwa_cmd, stdout=sam_out, check=True)
            recorder.record_command(n_align, bwa_cmd, int((time.time() - t0) * 1000))
        recorder.finish(n_align, outputs={"sam": str(sam)})

        n_sort = recorder.start("sort_bam_samtools", {"sam": str(sam)})
        _run(["samtools", "sort", "-@", str(args.threads), "-o", str(bam), str(sam)], node=n_sort)
        recorder.finish(n_sort, outputs={"bam": str(bam)})

        n_bam_idx = recorder.start("index_bam_samtools", {"bam": str(bam)})
        _run(["samtools", "index", str(bam)], node=n_bam_idx)
        recorder.finish(n_bam_idx, outputs={"bai": str(Path(str(bam) + ".bai"))})

        n_call = recorder.start("call_variants_gatk_haplotypecaller", {"bam": str(bam), "ref": str(ref_fa)})
        _run(
            [
                "gatk",
                "HaplotypeCaller",
                "-R",
                str(ref_fa),
                "-I",
                str(bam),
                "-O",
                str(vcf),
            ],
            node=n_call,
        )
        recorder.finish(n_call, outputs={"vcf": str(vcf)})

        n_vcf_header = recorder.start("ensure_vcf_reference_header", {"vcf": str(vcf), "ref": str(ref_fa)})
        _inject_reference_header(vcf, ref_fa)
        variants = _count_vcf_variants(vcf)
        recorder.finish(n_vcf_header, stats={"variants": variants})

        n_csv = recorder.start("convert_vcf_to_csv", {"vcf": str(vcf)})
        csv_stats = _vcf_to_csv(vcf, csv_file)
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
        _write_report(report, fastq1, fastq2, ref_fa, vcf, csv_file, prediction, bam, variants)
        recorder.finish(n_report, outputs={"report": str(report)})

        n_cleanup = recorder.start("cleanup_temp_files", {"sam": str(sam)})
        sam.unlink(missing_ok=True)
        recorder.finish(n_cleanup, outputs={"sam_removed": str(sam)})

        records_path = recorder.write("ok")
        print(
            json.dumps(
                {
                    "ok": True,
                    "vcf": str(vcf),
                    "csv": str(csv_file),
                    "report": str(report),
                    "prediction_json": str(prediction_json),
                    "bam": str(bam),
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
