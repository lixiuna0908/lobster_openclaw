# 疾病关联报告

## Pipeline 概览
- FASTQ 输入: `/tmp/lobster_fastq_to_disease_report_20260224/fastq/sample1.fastq`
- VCF 输出: `/tmp/lobster_fastq_to_disease_report_20260224/vcf/sample1.variants.vcf`
- 突变表: `/tmp/lobster_fastq_to_disease_report_20260224/reports/mutations.csv`
- 变异调用数: 4
- VEP 注释率: 100.0%
- ClinVar 命中: 4 / 4

## 高优先级突变（按 priority_score）
- `rs121913529` | KRAS | 12:25245350 C>A | score=0.700 | ClinVar=not_provided,likely_pathogenic,pathogenic,association
  - 关联疾病（MyVariant/ClinVar 条件）: Carcinoma of pancreas, Cerebral arteriovenous malformation (BAVM), Chronic myelogenous leukemia, BCR-ABL1 positive (CML), Juvenile myelomonocytic leukemia (JMML), Linear nevus sebaceous syndrome, Lung sarcomatoid carcinoma, Nevus sebaceous, Non-small cell lung carcinoma (NSCLC)
- `rs113488022` | BRAF | 7:140753336 A>C | score=0.700 | ClinVar=uncertain_significance,not_provided,likely_pathogenic,pathogenic
- `rs28934578` | TP53 | 17:7675088 C>A | score=0.700 | ClinVar=likely_pathogenic,pathogenic
- `rs429358` | APOE | 19:44908684 T>C | score=0.550 | ClinVar=uncertain_significance,not_provided,likely_pathogenic,pathogenic,drug_response,other,risk_factor,association,protective,established_risk_allele
  - 关联疾病（MyVariant/ClinVar 条件）: APOE4(-)-FREIBURG, APOE5 VARIANT, Alzheimer disease, Alzheimer disease 2 (AD2), Alzheimer disease 4 (AD4), Familial hypercholesterolemia, Familial type 3 hyperlipoproteinemia, Lipoprotein glomerulopathy (LPG)

## 解释与注意事项
- 本次 FASTQ→VCF 为可复现实验流程（toy caller）用于验证端到端链路；后续可替换为 BWA+GATK/bcftools 等生产级 calling。
- 疾病关联来源于 ClinVar 注释与 MyVariant 聚合条件信息，属于研究证据汇总，不构成临床诊断。