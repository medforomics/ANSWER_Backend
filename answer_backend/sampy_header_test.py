from pysam import VariantFile

variants = VariantFile("348794361-91944182.utsw.vcf.gz")
unique_info_keys = set()
fpout = open('bad_records.txt','w')
for record in variants:
    info_keys = []
    for key, value in record.info.items():
        unique_info_keys.add(key)
        info_keys.append(key)
    if 'ANN' not in info_keys:
        fpout.write(str(record))

fpout.close()
problematic_keys = []
multi_allele_numbers = ['A','.','R']
for key in sorted(unique_info_keys):
    if variants.header.info.get(key).number in multi_allele_numbers:
        print(key)
        problematic_keys.append(key)
print(len(problematic_keys),"problematic keys.")
print(variants.header)