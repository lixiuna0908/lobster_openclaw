#!/bin/bash

# 这是一个自动下载公共可访问的生信疾病预测/临床解读数据库的脚本
# 注意：许多高级数据库（OMIM, COSMIC, HGMD, OncoKB等）需要商业授权或学术注册，无法直接通过脚本匿名下载。

set -e

echo "=========================================================="
echo "开始下载公共可访问的临床解读知识库 (Open Knowledge DBs)"
echo "=========================================================="

# 1. ClinVar (全球最大的临床变异致病性共享数据库)
echo "[1/3] 正在下载 ClinVar (GRCh38版本 VCF格式)..."
mkdir -p clinvar
cd clinvar
curl -# -O https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz
curl -# -O https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz.tbi
cd ..
echo "ClinVar 下载完成。"

# 2. CIViC (临床解释变异联盟 - 专注于肿瘤)
echo "[2/3] 正在下载 CIViC 肿瘤变异临床证据库 (TSV格式)..."
mkdir -p civic
cd civic
curl -# -o nightly-ClinicalEvidenceSummaries.tsv https://civicdb.org/downloads/nightly/nightly-ClinicalEvidenceSummaries.tsv
cd ..
echo "CIViC 下载完成。"

# 3. PharmGKB (药物基因组学知识库)
echo "[3/3] 正在下载 PharmGKB 临床用药注释库 (ZIP格式)..."
mkdir -p pharmgkb
cd pharmgkb
curl -# -O https://s3.pgkb.org/data/clinicalAnnotations.zip
unzip -q -o clinicalAnnotations.zip
cd ..
echo "PharmGKB 下载完成并已解压。"

echo "=========================================================="
echo "所有公共数据库下载与初步整合完成！"
echo "请查看 openKnowledgeDB 文件夹下的内容。"
echo "=========================================================="
