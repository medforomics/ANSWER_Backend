class Virus(object):
    def __init__(self, row, case_id):
        # SampleID        VirusName       VirusAcc        VirusDescription        ViralReadCt
        self.case_id = case_id
        self.reference_id = None
        self.utsw_annotated = False
        self.mda_annotated = False
        self.likely_artifact = False
        self.num_cases_seen = 0
        self.sample_id = row.get('SampleID')
        self.virus_name = row.get('VirusName')
        self.virus_acc = row.get('VirusAcc')
        self.virus_description = row.get('VirusDescription')
        self.virus_read_count = row.get('ViralReadCt')

    def as_mongo_dict(self):
        mongo_dict = {
            'caseId': self.case_id,
            'referenceId': self.reference_id,
            'utswAnnotated': self.utsw_annotated,
            'mdaAnnotated': self.mda_annotated,
            'SampleId': self.sample_id,
            'VirusName': self.virus_name,
            'VirusAcc': self.virus_acc,
            'VirusDescription': self.virus_description,
            'VirusReadCount': self.virus_read_count,
            'numCasesSeen': self.num_cases_seen,
        }
        return mongo_dict
