# Header: Gene	Chromosome	Start	End	Abberation Type	CN	Score
GENE_KEY_NAMES = ['ensembl_gn', 'identifier']


class CopyNumberVariation:
    def __init__(self, gene_array, line=None, case_id=None, reference_id=None, cnv_dict=None):
        self.genes = gene_array
        self.case_id = case_id
        self.reference_id = reference_id
        self.likely_artifact = False
        self.utsw_annotated = False
        self.mda_annotated = False
        if line is not None:
            array = line.strip().split('\t')
            self.chrom = array[1]
            try:
                self.start = int(array[2])
            except ValueError:
                self.start = None
                # self.start = array[2]
            try:
                self.end = int(array[3])
            except ValueError:
                self.end = None
                # self.end = array[3]
            self.aberration_type = array[4]
            try:
                self.copy_number = float(array[5])
            except ValueError:
                self.copy_number = None
            try:
                self.score = float(array[6])
            except ValueError:
                self.score = None
            self.cytoband = array[7]
        else:
            self.chrom = cnv_dict['chrom']
            self.start = cnv_dict['start']
            self.end = cnv_dict['end']
            self.aberration_type = cnv_dict['aberrationType']
            self.copy_number = cnv_dict['copyNumber']
            self.score = None
            self.cytoband = cnv_dict['cytoband']
            self.highest_tier = None

    def as_mongo_dict(self):
        return {'genes': self.genes, 'chrom': self.chrom, 'start': self.start, 'end': self.end,
                'copyNumber': self.copy_number, 'aberrationType': self.aberration_type, 'caseId': self.case_id,
                'referenceId': self.reference_id, 'score': self.score, 'utswAnnotated': self.utsw_annotated,
                'mdaAnnotated': self.mda_annotated,
                'cytoband': self.cytoband,
                'selected': False,
                'likelyArtifact': self.likely_artifact,
                'highestTier': self.highest_tier,
                }
