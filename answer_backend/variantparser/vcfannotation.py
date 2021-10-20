IMPACT = {
    'HIGH': 0,
    'MODERATE': 1,
    'LOW': 2,
    'MODIFIER': 3,
}

# Reversed for easy comparisons, 1 is more support
TSL = {
    '1': 0,
    '2': 1,
    '3': 2,
    '4': 3,
    '5': 4,
    '-': 5,
}

# Reversed for easy comparisons, PRINCIPAL:1 has most support
APPRIS = {
    'PRINCIPAL:1': 0,
    'PRINCIPAL:2': 1,
    'PRINCIPAL:3': 2,
    'PRINCIPAL:4': 3,
    'PRINCIPAL:5': 4,
    'ALTERNATIVE:1': 5,
    'ALTERNATIVE:2': 6,
    'MINOR': 7,
    'None': 8,
}


class VcfAnnotation(object):
    """
    Represents a standard VCF Annotation field version 1.0
    """

    def __init__(self, ann_data):
        self.allele = ann_data['allele']
        self.effects = ann_data['effects']
        self.impact = ann_data['impact']
        if 'structural_interaction_variant' in self.effects:
            self.impact = 'MODERATE'
        if 'protein_protein_contact' in self.effects:
            self.impact = 'MODERATE'
        self.gene_name = ann_data['gene_name']
        self.gene_id = ann_data['gene_id']
        self.feature_type = ann_data['feature_type']
        self.feature_id = ann_data['feature_id']
        self.transcript_biotype = ann_data['transcript_biotype']
        self.rank = ann_data['rank']
        self.coding_notation = ann_data['coding_notation']
        self.protein_notation = ann_data['protein_notation']
        self.cdna_position = ann_data['cdna_position']
        self.cds_position = ann_data['cds_position']
        self.protein_position = ann_data['protein_position']
        self.distance_to_feature = ann_data['distance_to_feature']
        self.appris = 'None'
        self.tsl = '-'
        pass

    @classmethod
    def from_annotation(cls, annotation):
        ann_data = {}
        ann_array = annotation.split("|")
        ann_data['allele'] = ann_array[0]
        ann_data['effects'] = ann_array[1].split('&')
        ann_data['impact'] = ann_array[2]
        ann_data['gene_name'] = ann_array[3]
        ann_data['gene_id'] = ann_array[4]
        ann_data['feature_type'] = ann_array[5]
        ann_data['feature_id'] = ann_array[6]
        ann_data['transcript_biotype'] = ann_array[7]
        ann_data['rank'] = ann_array[8]
        ann_data['coding_notation'] = ann_array[9]
        ann_data['protein_notation'] = ann_array[10]
        ann_data['cdna_position'] = ann_array[11]
        ann_data['cds_position'] = ann_array[12]
        ann_data['protein_position'] = ann_array[13]
        ann_data['distance_to_feature'] = ann_array[14]
        return cls(ann_data)

    def as_mongo_dict(self):
        mongo_dict = {'allele': self.allele, 'effects': self.effects, 'impact': self.impact, 'geneName': self.gene_name,
                      'geneId': self.gene_id, 'featureType': self.feature_type, 'featureId': self.feature_id,
                      'transcriptBiotype': self.transcript_biotype, 'rank': self.rank,
                      'codingNotation': self.coding_notation, 'proteinNotation': self.protein_notation,
                      'proteinPosition': self.protein_position, 'cdnaPosition': self.cdna_position,
                      'cdsPosition': self.cds_position, 'distanceToFeature': self.distance_to_feature,
                      'appris': self.appris, 'tsl': self.tsl}
        return mongo_dict

    def __lt__(self, other):
        # Return true if self is less than other
        if IMPACT[self.impact] < IMPACT[other.impact]:
            # if 'structural_interaction_variant' in other.effect:
            #     pass
            # else:
            return True
        if APPRIS[self.appris] < APPRIS[other.appris]:
            return True
        if TSL[self.tsl] < TSL[other.tsl]:
            return True
        return False

    def assign_appris_data(self, db):
        transcript = self.feature_id.split('.')[0]
        self.appris, self.tsl = db.fetch_appris_data(transcript)
        pass
