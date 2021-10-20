class ReferenceVariant(object):
    def __init__(self, mongo_dict):
        self.chrom = mongo_dict['chrom']
        self.pos = mongo_dict['pos']
        self.alt = mongo_dict['alt']
        self.reference = mongo_dict['reference']
        self.type = mongo_dict['type']
        self.annotations = mongo_dict.get('annotations', [])
        self.utsw_annotations = mongo_dict.get('utswAnnotations', [])
        self.clinical_cases = mongo_dict.get('clinicalCases', [])
        self.research_cases = mongo_dict.get('researchCases', [])
        self.mongo_id = mongo_dict.get('_id', None)
        self.old_builds = mongo_dict.get('oldBuilds', {})

    @classmethod
    def from_record(cls, record):
        info_dict = {}
        for key, value in record.info.items():
            if key == 'ANN':
                pass
            else:
                info_dict[key.replace('.', '_')] = value

        mongo_dict = {
            'chrom': record.chrom,
            'pos': record.pos,
            'alt': record.alts[0],
            'reference': record.alleles[0],
            'type': info_dict.get('TYPE', ['Unknown'])[0],
            'annotations': [],
            'utswAnnotations': [],
            'clinicalCases': [],
            'researchCases': [],
            'oldBuilds': {},
        }

        return cls(mongo_dict)

    @classmethod
    def from_variant(cls, variant):
        mongo_dict = {
            'chrom': variant.chrom,
            'pos': variant.pos,
            'alt': variant.alt,
            'reference': variant.reference,
            'type': variant.type,
            'annotations': [],
            'utswAnnotations': [],
            'clinicalCases': [],
            'researchCases': [],
            'oldBuilds': {},
        }
        return cls(mongo_dict)

    def as_mongo_dict(self):
        mongo_dict = {'chrom': self.chrom, 'pos': self.pos, 'alt': self.alt, 'reference': self.reference,
                      'type': self.type, 'clinicalCases': self.clinical_cases, 'researchCases': self.research_cases,
                      'annotations': self.annotations, 'utswAnnotations': self.utsw_annotations,
                      'oldBuilds': self.old_builds}
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
            db.reference_variants.find_one_and_update({'_id': self.mongo_id}, {'$set': update})

    def set_hg19(self, hg38_to_19):
        hg19_liftover = hg38_to_19.convert_coordinate(self.chrom, self.pos, '+')
        try:
            hg19_position = hg19_liftover[0]
            self.old_builds['hg19'] = {
                'chrom': hg19_position[0],
                'pos': hg19_position[1]
            }
        except (IndexError,TypeError):
            self.old_builds['hg19'] = None
            pass
