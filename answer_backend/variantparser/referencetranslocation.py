class ReferenceTranslocation:
    def __init__(self, mongo_dict):
        self.gene_names = mongo_dict['geneNames']
        self.clinical_cases = mongo_dict['clinicalCases']
        self.research_cases = mongo_dict['researchCases']
        self.annotations = mongo_dict.get('annotations', [])
        self.mongo_id = mongo_dict.get('_id', None)

    @classmethod
    def from_translocation(cls, translocation):
        mongo_dict = {
            'geneNames': [translocation.left_gene, translocation.right_gene],
            'clinicalCases': [],
            'researchCases': [],
        }
        return cls(mongo_dict)

    @classmethod
    def from_manual_entry(cls, left_gene, right_gene):
        mongo_dict = {
            'geneNames': [left_gene, right_gene],
            'clinicalCases': [],
            'researchCases': [],
        }
        return cls(mongo_dict)

    def as_mongo_dict(self):
        mongo_dict = {
            'geneNames': self.gene_names, 'clinicalCases': self.clinical_cases,
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
            db.reference_translocations.find_one_and_update({'_id': self.mongo_id}, {'$set': update})
