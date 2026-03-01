import pysam
vcf_in = pysam.VariantFile('/Users/work/000code/github/test_data/out_dingtalk/sample1.variants.raw.vcf')
# create output header with some additional INFO
new_header = vcf_in.header.copy()
new_header.info.add('CNN_1D', '.', 'Float', 'CNN 1D score')

vcf_out = pysam.VariantFile('test_out.vcf', 'w', header=new_header)
for rec in vcf_in:
    new_rec = vcf_out.new_record()
    new_rec.contig = rec.contig
    new_rec.pos = rec.pos
    new_rec.id = rec.id
    new_rec.ref = rec.ref
    new_rec.alts = rec.alts
    new_rec.qual = rec.qual
    new_rec.filter.clear()
    for f in rec.filter:
        new_rec.filter.add(f)
    for k, v in rec.info.items():
        new_rec.info[k] = v
    new_rec.info['CNN_1D'] = 1.0
    vcf_out.write(new_rec)
vcf_in.close()
vcf_out.close()
print("Success")
