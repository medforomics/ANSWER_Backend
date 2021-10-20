class BAlleleFrequency(object):
    def __init__(self, row, case_id):
        self.case_id = case_id
        self.chrom = row['CHROM']
        self.pos = row['POS']
        self.ao = row['AO']
        self.ro = row['RO']
        self.dp = row['DP']
        self.maf = row['MAF']

    def as_mongo_dict(self):
        return ({'caseId': self.case_id,
                 'chrom': self.chrom,
                 'pos': self.pos,
                 'ao': self.ao,
                 'ro': self.ro,
                 'dp': self.dp,
                 'maf': self.maf})