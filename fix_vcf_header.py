import pysam
import sys

def fix_header(input_vcf, output_vcf):
    print(f"Fixing VCF header for {input_vcf}")
    # Open the input VCF
    with pysam.VariantFile(input_vcf, 'r') as vcf_in:
        # Create a new header and add the missing fields
        new_header = vcf_in.header.copy()
        
        # Ensure all standard GATK info fields are present
        standard_info = [
            ('AC', 'A', 'Integer', 'Allele count in genotypes'),
            ('AF', 'A', 'Float', 'Allele Frequency'),
            ('AN', '1', 'Integer', 'Total number of alleles'),
            ('DP', '1', 'Integer', 'Approximate read depth'),
            ('BaseQRankSum', '1', 'Float', 'Z-score from Wilcoxon rank sum test of Alt Vs. Ref base qualities'),
            ('ExcessHet', '1', 'Float', 'Phred-scaled p-value for exact test of excess heterozygosity'),
            ('FS', '1', 'Float', 'Phred-scaled p-value using Fisher exact test to detect strand bias'),
            ('InbreedingCoeff', '1', 'Float', 'Inbreeding coefficient'),
            ('MLEAC', 'A', 'Integer', 'Maximum likelihood expectation (MLE) for the allele counts'),
            ('MLEAF', 'A', 'Float', 'Maximum likelihood expectation (MLE) for the allele frequency'),
            ('MQ', '1', 'Float', 'RMS Mapping Quality'),
            ('MQRankSum', '1', 'Float', 'Z-score From Wilcoxon rank sum test of Alt vs. Ref read mapping qualities'),
            ('QD', '1', 'Float', 'Variant Confidence/Quality by Depth'),
            ('ReadPosRankSum', '1', 'Float', 'Z-score from Wilcoxon rank sum test of Alt vs. Ref read position bias'),
            ('SOR', '1', 'Float', 'Symmetric Odds Ratio of 2x2 contingency table to detect strand bias')
        ]
        
        for id, num, type, desc in standard_info:
            try:
                new_header.info.add(id, num, type, desc)
            except Exception:
                pass # Already exists
                
        standard_format = [
            ('GT', '1', 'String', 'Genotype'),
            ('AD', 'R', 'Integer', 'Allelic depths'),
            ('DP', '1', 'Integer', 'Approximate read depth'),
            ('GQ', '1', 'Integer', 'Genotype Quality'),
            ('PL', 'G', 'Integer', 'Normalized, Phred-scaled likelihoods')
        ]
        
        for id, num, type, desc in standard_format:
            try:
                new_header.formats.add(id, num, type, desc)
            except Exception:
                pass
                
        # Write to the new VCF
        with pysam.VariantFile(output_vcf, 'w', header=new_header) as vcf_out:
            for record in vcf_in:
                vcf_out.write(record)

if __name__ == "__main__":
    fix_header(sys.argv[1], sys.argv[2])
