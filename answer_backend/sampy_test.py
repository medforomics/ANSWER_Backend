from pysam import VariantFile

variants = VariantFile("341642139-92166634.vcf.gz")

record = next(variants.fetch())
for record in variants:
    print(record)
    for idx, sample in enumerate(record.samples):
        if "T_DNA" in sample:
            t_dna = idx
        if "N_DNA" in sample:
            n_dna = idx
        if "T_RNA" in sample:
            t_rna = idx
        # record.samples.items()[sample_number].items() gives you the tuple of genotype fields
    tumor_alt_depth = record.samples.items()[t_dna][1].items()[2][1][1]
    tumor_total_depth = record.samples.items()[t_dna][1].items()[1][1]
    tumor_alt_frequency = tumor_alt_depth / tumor_total_depth

    if n_dna is not None:
        normal_alt_depth = record.samples.items()[n_dna][1].items()[2][1][1]
        normal_total_depth = record.samples.items()[n_dna][1].items()[1][1]
        normal_alt_frequency = normal_alt_depth / normal_total_depth

    if t_rna is not None:
        print(record.samples.items()[t_rna][1].items()[2][1][0])
        if record.samples.items()[t_rna][1].items()[2][1][0] is not None:
            rna_alt_depth = record.samples.items()[t_rna][1].items()[2][1][1]
        if record.samples.items()[t_rna][1].items()[1][0] is not None:
            rna_total_depth = record.samples.items()[t_rna][1].items()[1][1]
        rna_alt_frequency = rna_alt_depth / rna_total_depth
types_found = 0
no_types_found = 0
i = 0

# unique_info_keys = set()
# for record in variants:
#     info_keys = []
#     for key, value in record.info.items():
#         unique_info_keys.add(key)
#         info_keys.append(key)
#     print(info_keys)
#     if 'CNT' in info_keys:
#         print(type(record.info['CNT']))
#     i = i + 1
#     #print(record.ID)
#     #print(record)
#     #print(record.INFO['ANN'])
#     if len(record.alts) > 1:
#         print("Found record with more than 1 alt")
#         print(record)
#         if 'TYPE' not in record.info:
#             no_types_found += 1
#             print("No TYPE Found")
#         else:
#             print("TYPE Found")
#             types_found += 1
# print('Alleles:',record.alleles)
# print('Alts:',record.alts)
# new_record = record.copy()
# new_record.alts = (record.alts[0])
# print('new record:',new_record.alleles)
# print("AD:",record.info['AD'])
# print('new record AD:',new_record.info['AD'])
# #array = record.info['ANN'][0].split("|")
# assert len(array) == 16
# if array[3] == 'FLT3':
#    print('Found: ',array[3])
#    print(record.info.items())
#    print(record.id)
#    print(record.alleles[0])
# try:
#    if 'Ser1870' in ','.join(record.info['ANN']):
#        print("Found",array[3])
#        print(record.id)
#        print(record.info.items())
#        print(array[9])
#        print(record.info['ANN'])
# except KeyError:
#    pass

# problematic_keys = []
# for key in sorted(unique_info_keys):
#     print(key)
# print(variants.header.info.get('AC').number)
#
# print("Total types found:",types_found)
# print("Total without types:",no_types_found)
# print(test_record)
# print(test_record.info.items())
# print(test_record.format.items())
