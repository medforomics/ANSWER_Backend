from pysam import VariantFile

variants = VariantFile("352770619-93528065.sssom.vcf.gz")

record = next(variants.fetch())
for record in variants:
    if(len(record.alts) > 1):
        if("PASS" in record.filter):
            print(record)



#     for idx, sample in enumerate(record.samples):
#         if "T_DNA" in sample:
#             t_dna = idx
#         if "N_DNA" in sample:
#             n_dna = idx
#         if "T_RNA" in sample:
#             t_rna = idx
#         # record.samples.items()[sample_number].items() gives you the tuple of genotype fields
#     tumor_alt_depth = record.samples.items()[t_dna][1].items()[2][1][1]
#     tumor_total_depth = record.samples.items()[t_dna][1].items()[1][1]
#     tumor_alt_frequency = tumor_alt_depth / tumor_total_depth
#
#     if n_dna is not None:
#         normal_alt_depth = record.samples.items()[n_dna][1].items()[2][1][1]
#         normal_total_depth = record.samples.items()[n_dna][1].items()[1][1]
#         normal_alt_frequency = normal_alt_depth / normal_total_depth
#
#     if t_rna is not None:
#         print(record.samples.items()[t_rna][1].items()[2][1][0])
#         if record.samples.items()[t_rna][1].items()[2][1][0] is not None:
#             rna_alt_depth = record.samples.items()[t_rna][1].items()[2][1][1]
#         if record.samples.items()[t_rna][1].items()[1][0] is not None:
#             rna_total_depth = record.samples.items()[t_rna][1].items()[1][1]
#         rna_alt_frequency = rna_alt_depth / rna_total_depth
# types_found = 0
# no_types_found = 0
# i = 0
