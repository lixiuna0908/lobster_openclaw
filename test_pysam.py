import pysam

# test basic pysam functionality
vcf_in = pysam.VariantFile('/Users/work/000code/github/test_data/out_dingtalk/sample1.variants.raw.vcf')
print(f"Opened VCF successfully. Num contigs: {len(vcf_in.header.contigs)}")
