class MutationalSignature(object):
    def __init__(self, row, case_id):
        # Signature       Proposed_Etiology       X{epicorder#}.{mrn}
        self.case_id = case_id
        self.signature = row['Signature']
        self.proposed_etiology = row['Proposed_Etiology']
        self.value = row.iloc[2]  # This seems pretty bad

    def as_mongo_dict(self):
        mongo_dict = {
            'caseId': self.case_id,
            'signature': self.signature,
            'proposedEtiology': self.proposed_etiology,
            'value': self.value
        }
        return mongo_dict
