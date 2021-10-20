class Fpkm(object):
    def __init__(self, row, case_id, oncotree_diagnosis):
        # Gene ID Gene Name       Reference       Strand  Start   End     Coverage        FPKM    TPM
        self.case_id = case_id
        self.oncotree_diagnosis = oncotree_diagnosis
        self.gene_id = row.get('Gene ID')
        self.gene_name = row.get('Gene Name')
        self.chrom = row.get('Reference')
        self.strand = row.get('Strand')
        self.start = int(row.get('Start'))
        self.end = int(row.get('End'))
        self.coverage = float(row.get('Coverage'))
        self.fpkm = float(row.get('FPKM'))
        self.tpm = float(row.get('TPM'))

    def as_mongo_dict(self):
        mongo_dict = {
            'caseId': self.case_id,
            'oncotreeDiagnosis': self.oncotree_diagnosis,
            'geneId': self.gene_id,
            'geneName': self.gene_name,
            'chrom': self.chrom,
            'strand': self.strand,
            'start': self.start,
            'end': self.end,
            'coverage': self.coverage,
            'fpkm': self.fpkm,
            'tpm': self.tpm,
        }
        return mongo_dict
