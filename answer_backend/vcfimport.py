from pysam import VariantFile
import json

variants = VariantFile("m16-3126.utsw.vcf")

test = variants.fetch()
record = next(test)

print(record)
print(record.info.items())
print(record.info['ANN'][0])
print(record.id)

print("record:",record)
print("record.chrom:",record.chrom)
print("record.start:",record.start)
print("record.stop:",record.stop)
print("record.pos:",record.pos)
print("record.id:",record.id)
print("record.alleles:",record.alleles)
print("record.info:",record.info)
print("record.ref:",record.ref)
print("record.qual:",record.qual)
print("record.filter.keys():",record.filter.keys())