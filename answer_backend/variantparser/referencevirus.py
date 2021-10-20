class ReferenceVirus:
    def __init__(self, mongo_dict):
        self.virus_name = mongo_dict['virusName']
        self.virus_acc = mongo_dict['virusAcc']
        self.virus_description = mongo_dict['virusDescription']
        self.clinical_cases = mongo_dict['clinicalCases']
        self.research_cases = mongo_dict['researchCases']
        self.annotations = mongo_dict['annotations']
        self.mongo_id = mongo_dict.get('_id', None)

    @classmethod
    def from_virus(cls, virus):
        mongo_dict = {
            'virusName': virus.virus_name,
            'virusAcc': virus.virus_acc,
            'virusDescription': virus.virus_description,
            'annotations': [],
            'clinicalCases': [],
            'researchCases': [],
        }
        return cls(mongo_dict)

    def as_mongo_dict(self):
        mongo_dict = {
            'virusAcc': self.virus_acc,
            'virusDescription': self.virus_description,
            'virusName': self.virus_name, 'clinicalCases': self.clinical_cases,
            'researchCases': self.research_cases, 'annotations': self.annotations
        }
        if self.mongo_id is not None:
            mongo_dict['_id'] = self.mongo_id
        return mongo_dict

    def assign_case(self, case_id, type, db):
        update = None
        if type == 'Clinical':
            if case_id not in self.clinical_cases:
                self.clinical_cases.append(case_id)
                update = {'clinicalCases': self.clinical_cases}
        else:
            if case_id not in self.research_cases:
                self.research_cases.append(case_id)
                update = {'researchCases': self.research_cases}
        if update:
            db.reference_viruses.find_one_and_update({'_id': self.mongo_id}, {'$set': update})
