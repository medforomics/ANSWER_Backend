# "FusionName","LeftGene","LeftBreakpoint","LeftGeneExons","LeftStrand","RightGene","RightBreakpoint","RightGeneExons","RightStrand","RNAReads","DNAReads","NormalDNAReads","SomaticStatus","FusionType","Annot",'Filter','ChrType','ChrDistance'
# DNAReads
class Translocation:
    def __init__(self, line=None, case_id=None, reference_id=None, ftl_dict=None):
        self.case_id = case_id
        self.reference_id = None
        self.utsw_annotated = False
        self.mda_annotated = False
        self.likely_artifact = False
        self.percent_supporting_reads = None
        if line is not None:
            # array = line.strip().split('\t')
            # if len(array) < 18:
            #     print(len(array), ":", line)
            #     print(array)
            # self.fusion_name = array[0]
            self.fusion_name = line.get('FusionName')
            # self.left_gene = array[1]
            self.left_gene = line.get('LeftGene')
            # self.left_breakpoint = array[2]
            self.left_breakpoint = line.get('LeftBreakpoint')
            if self.left_breakpoint is None:
                self.left_breakpoint = line.get('LefttBreakpoint')
            # self.left_exons = array[3]
            self.left_exons = str(line.get('LeftGeneExons'))
            # self.left_strand = array[4]
            self.left_strand = line.get('LeftStrand')
            # self.right_gene = array[5]
            self.right_gene = line.get('RightGene')
            # self.right_breakpoint = array[6]
            self.right_breakpoint = line.get('RightBreakpoint')
            # self.right_exons = array[7]
            self.right_exons = str(line.get('RightGeneExons'))
            # self.right_strand = array[8]
            self.right_strand = line.get('RightStrand')
            try:
                # self.rna_reads = int(array[9])
                self.rna_reads = int(line.get('RNAReads'))
            except (ValueError, TypeError):
                # self.rna_reads = line.get('RNAReads')
                self.rna_reads = None
            try:
                self.dna_reads = int(line.get('DNAReads'))
                # self.dna_reads = int(array[10])
            except (ValueError, TypeError):
                # self.dna_reads = line.get('DNAReads')
                self.dna_reads = None
            try:
                # self.normal_dna_reads = int(array[11])
                self.normal_dna_reads = int(line.get('NormalDNAReads'))
            except (ValueError, TypeError):
                # self.normal_dna_reads = line.get('NormalDNAReads')
                self.normal_dna_reads = None
            # self.somatic_status = array[12]
            self.somatic_status = line.get('SomaticStatus')
            # self.fusion_type = array[13]
            self.fusion_type = line.get('FusionType')
            # self.annot = array[14]
            self.annot = line.get('Annot')
            # self.filters = array[15].split(';')
            self.filters = line.get('Filter').split(';')
            # self.chr_type = array[16]
            self.chr_type = line.get('ChrType')
            # self.chr_distance = array[17]
            self.chr_distance = line.get('ChrDistance')
            #    try:
            #        self.percent_supporting_reads = array[16]
            #    except IndexError:
            #        pass

        else:
            self.fusion_name = ftl_dict['fusionName']
            self.left_gene = ftl_dict['leftGene']
            self.right_gene = ftl_dict['rightGene']
            self.left_exons = ftl_dict['leftExons']
            self.right_exons = ftl_dict['rightExons']
            self.left_breakpoint = None
            self.right_breakpoint = None
            self.left_strand = None
            self.right_strand = None
            self.rna_reads = None
            self.dna_reads = None
            self.filters = []
            self.chr_type = None
            self.chr_distance = None
            self.fusion_type = None
            self.annot = None
            self.normal_dna_reads = None
            self.somatic_status = None

        self.gene_names = [self.left_gene, self.right_gene]
        self.num_cases_seen = 0
        self.highest_tier = None

    def as_mongo_dict(self):
        return {'fusionName': self.fusion_name,
                'leftGene': self.left_gene,
                'rightGene': self.right_gene,
                'leftBreakpoint': self.left_breakpoint,
                'leftExons': self.left_exons,
                'rightExons': self.right_exons,
                'rightBreakpoint': self.right_breakpoint,
                'leftStrand': self.left_strand,
                'rightStrand': self.right_strand,
                'rnaReads': self.rna_reads,
                'dnaReads': self.dna_reads,
                'filters': self.filters,
                'chrType': self.chr_type,
                'chrDistance': self.chr_distance,
                'percentSupportingReads': self.percent_supporting_reads,
                'caseId': self.case_id,
                'referenceId': self.reference_id,
                'numCasesSeen': self.num_cases_seen,
                'fusionType': self.fusion_type,
                'annot': self.annot,
                'utswAnnotated': self.utsw_annotated,
                'mdaAnnotated': self.mda_annotated,
                'likelyArtifact': self.likely_artifact,
                'normalDnaReads': self.normal_dna_reads,
                'somaticStatus': self.somatic_status,
                'highestTier': self.highest_tier
                }
