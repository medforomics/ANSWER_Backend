class ReferenceExonSkip:
    def __init__(self, mongo_dict):
        self.gene_names = mongo_dict['geneNames']
        self.aberration_type = mongo_dict['aberrationType']
        self.clinical_cases = mongo_dict['clinicalCases']
        self.research_cases = mongo_dict['researchCases']
        self.annotations = []
        self.mongo_id = mongo_dict.get('_id', None)

    @classmethod
    def from_copy_number_variation(cls, cnv):
        mongo_dict = {
            'geneNames': cnv.genes,
            'aberrationType': cnv.aberration_type,
            'clinicalCases': [],
            'researchCases': [],
            'annotations': [],
        }
        return cls(mongo_dict)

    def as_mongo_dict(self):
        mongo_dict = {
            'geneNames': self.gene_names, 'clinicalCases': self.clinical_cases, 'researchCases': self.research_cases,
            'aberrationType': self.aberration_type, }
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
            db.reference_cnv.find_one_and_update({'_id': self.mongo_id}, {'$set': update})
