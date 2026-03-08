# 疾病关联分析报告 (Disease Association Report)

## 输入文件 (Inputs)
- FASTQ R1: `/Users/work/000code/github/test_data/sample1.fastq`
- FASTQ R2: `N/A (单端/single-end)`
- 参考基因组 (Reference): `/Users/work/000code/github/refer_hg/hg38/hg38.fa`

## 分析流程 (Pipeline)
- 质控与过滤 (FASTQ QC + trimming): fastp
- 序列比对 (Alignment): BWA-MEM
- BAM处理 (BAM processing): samtools sort/index + GATK MarkDuplicates
- 变异检测 (Variant calling): GATK HaplotypeCaller
- 变异过滤 (Variant filtering): GATK NVScoreVariants + FilterVariantTranches

## 输出文件 (Outputs)
- BAM: `/Users/work/000code/github/test_data/out/sample1.dedup.bam`
- VCF: `/Users/work/000code/github/test_data/out/sample1.variants.rare.vcf`
- CSV: `/Users/work/000code/github/test_data/out/mutations.csv`
- 有效变异数 (Variants): `0`

## 疾病预测结果 (Disease Prediction)
- 整体风险等级 (Overall risk level): `低风险(low)`
- 整体风险评分 (Overall score): `0.05`
- 平均等位基因频率 (Mean AF): `0.0`
- 高风险变异数 (High-risk variants): `0` / `0`

## 预测的高风险疾病 (Top Predicted Risks)
- 在ClinVar数据库中未发现已知的致病风险 (No known pathogenic risks found in ClinVar database).

## 节点运行记录 (Node Records)
- 完整的节点级别记录已写入输出目录中的 `pipeline_node_records.json` 文件 (Full node-level records are written to `pipeline_node_records.json` in the output directory).
