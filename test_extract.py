import re
from typing import Optional, Tuple

def _extract_paths(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    _eq = r"\s*[:=：＝]\s*"
    patterns = {
        "fastq": rf"(?:fastq1|fastq)\s*{_eq}([^\s]+)",
        "fastq2": rf"(?:fastq2)\s*{_eq}([^\s]+)",
        "ref": rf"(?:ref|reference|参考(?:基因组)?)\s*{_eq}([^\s]+)",
        "outdir": rf"(?:outdir|output|输出目录)\s*{_eq}([^\s]+)",
        "gnomad": rf"(?:gnomad)\s*{_eq}([^\s]+)",
    }
    fastq = None
    fastq2 = None
    ref = None
    outdir = None
    gnomad = None
    m = re.search(patterns["fastq"], text, flags=re.IGNORECASE)
    if m:
        fastq = m.group(1).strip()
    if not fastq:
        m = re.search(r"(?i)(?:fastq1|fastq)\s*[=:=：＝]\s*(\S+)", text)
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
    m = re.search(patterns["gnomad"], text, flags=re.IGNORECASE)
    if m:
        gnomad = m.group(1).strip()
    return fastq, fastq2, ref, outdir, gnomad

text = "@fastq处理 FASTQ1=/Users/work/000code/github/li/E250161160_L01_WH_WGS2601000422-2-8240_1.fq.gz  FASTQ2=/Users/work/000code/github/li/E250161160_L01_WH_WGS2601000422-2-8240_2.fq.gz 帮我运行"
print(_extract_paths(text))
def _should_run(text: str) -> bool:
    run_words = ["帮我运行", "运行流程", "开始处理", "开始运行", "run", "执行"]
    lower = text.lower()
    return any(w.lower() in lower for w in run_words)

print(_should_run(text))
