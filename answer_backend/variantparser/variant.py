from .vcfannotation import VcfAnnotation

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


class CaseLoadingError(Exception):
    def __init__(self, message):
        self.message = message


def vcf_to_number(number):
    if number == '.':
        return '0'
    else:
        return number


def parse_call_set(call_set):
    output = []
    for caller in call_set:
        array = caller.split('|')
        normal_array_index = None
        tumor_array_index = None
        if array[0] == 'pindel': # HACKHACK
            pass
        for index, item in enumerate(array):
            if '_N_' in item:
                normal_array_index = index
            if '_T_' in item:
                tumor_array_index = index
        tumor_total_depth = None
        tumor_allele_frequency = None
        normal_total_depth = None
        normal_allele_frequency = None
        if tumor_array_index:
            tumor_total_depth = int(vcf_to_number(array[tumor_array_index].split(':')[1]))
            tumor_allele_frequency = float(vcf_to_number(array[tumor_array_index].split(':')[2]))
        if normal_array_index:
            normal_total_depth = int(vcf_to_number(array[normal_array_index].split(':')[1]))
            normal_allele_frequency = float(vcf_to_number(array[normal_array_index].split(':')[2]))
        try:
            output_dict = {'callerName': array[0],
                           'alt': array[1],
                           'tumorTotalDepth': tumor_total_depth,
                           'tumorAlleleFrequency': tumor_allele_frequency,
                           'normalTotalDepth': normal_total_depth,
                           'normalAlleleFrequency': normal_allele_frequency,
                           }
        except IndexError:
            raise CaseLoadingError("Problem parsing CallSet in VCF file" + str(call_set))
        output.append(output_dict)
    return output


def sort_key(ann):
    return IMPACT[ann.impact], APPRIS[ann.appris], TSL[ann.tsl]


class Variant(object):

    def __init__(self, record, case_id, reference_id=None):
        self.reference_id = reference_id
        if record.id is not None:
            self.ids = record.id.split(';')
        else:
            self.ids = [None]
        clinvar = None
        for id_number in self.ids:
            if id_number is not None:
                try:
                    clinvar = int(id_number)
                except ValueError:
                    pass
        if clinvar is not None:
            self.in_clinvar = True
        else:
            self.in_clinvar = False
        # Handle new cosmic IDs.
        self.case_id = case_id
        self.chrom = record.chrom
        self.pos = record.pos
        self.old_builds = {}
        self.alt = record.alts[0]
        self.reference = record.alleles[0]
        self.selected = False
        self.info = record.info
        self.info_dict = {}
        for key, value in record.info.items():
            if key == 'ANN':
                pass
            else:
                self.info_dict[key.replace('.', '_')] = value
        cosmic_legacy_ids = self.info_dict.get('LEGACY_ID', ())
        if type(cosmic_legacy_ids) is tuple:
            self.ids += cosmic_legacy_ids
        else:
            self.ids += [cosmic_legacy_ids]
        self.filters = record.filter.keys()
        self.vcf_annotations = []
        for ann in record.info['ANN']:
            self.vcf_annotations.append(VcfAnnotation.from_annotation(ann))
        self.mda_annotation = None
        self.mda_annotated = False
        self.utsw_annotated = False
        self.likely_artifact = False
        self.gene_name = self.vcf_annotations[0].gene_name
        self.effects = self.vcf_annotations[0].effects
        self.impact = self.vcf_annotations[0].impact
        self.notation = self.vcf_annotations[0].protein_notation
        self.rank = self.vcf_annotations[0].rank
        if self.notation == '':
            self.notation = self.vcf_annotations[0].coding_notation
        self.oncokb_gene_name = None
        self.oncokb_variant_name = None
        self.is_oncokb_variant = False
        call_set = self.info_dict.get('CallSet', ())
        self.call_set = parse_call_set(call_set)

        # INFO field values
        self.type = self.info_dict.get('TYPE ', ['Unknown'])[0]
        self.cosmic_patients = self.info_dict.get('CNT', ())
        if not isinstance(self.cosmic_patients, tuple):
            self.cosmic_patients = (self.cosmic_patients,)
        try:
            self.max_cosmic_patients = max(self.cosmic_patients)
        except ValueError:
            self.max_cosmic_patients = 0
        self.exac_allele_frequency = self.info_dict.get('dbNSFP_ExAC_AF', [0.0])[0]
        somatic_value = self.info_dict.get('SS', 5)
        self.somatic_status = 'Unknown'
        if somatic_value == '1':
            self.somatic_status = 'Germline'
        elif somatic_value == '2':
            self.somatic_status = 'Somatic'
        elif somatic_value == '3':
            self.somatic_status = 'LOH'
        self.gnomad_popmax_af = self.info_dict.get('AF_POPMAX', [0.0])[0]
        self.gnomad_homozygotes = self.info_dict.get('GNOMAD_HOM', None)
        self.gnomad_hg19_variant = self.info_dict.get('GNOMAD_HG19_VARIANT', [None])[0]
        self.gnomad_lcr = self.info_dict.get('GNOMAD_LCR', [None])[0]
        self.repeat_types = self.info_dict.get('RepeatType', None)

        if self.repeat_types is not None:
            if isinstance(self.repeat_types, tuple):
                pass
            else:
                self.repeat_types = (self.repeat_types,)
            self.is_repeat = True
        else:
            self.repeat_types = []
            self.is_repeat = False
        callset_inconsistent = self.info_dict.get('CallSetInconsistent', 0)
        if callset_inconsistent == 1:
            self.callset_inconsistent = True
        else:
            self.callset_inconsistent = False
        t_dna = None
        n_dna = None
        t_rna = None

        for idx, sample in enumerate(record.samples):
            if "T_DNA" in sample:
                t_dna = idx
            if "N_DNA" in sample:
                n_dna = idx
            if "T_RNA" in sample:
                t_rna = idx
        #        if t_rna is None:
        #            print("No RNA found in case")
        # record.samples.items()[sample_number].items() gives you the tuple of genotype fields
        try:
            self.tumor_alt_depth = record.samples.items()[t_dna][1].items()[2][1][1]
            self.tumor_total_depth = record.samples.items()[t_dna][1].items()[1][1]
        except IndexError as e:
            print("Chromosome position at error:",self.chrom, self.pos)
            raise

        self.tumor_alt_frequency = self.tumor_alt_depth / self.tumor_total_depth

        self.normal_alt_depth = None
        self.normal_total_depth = None
        self.normal_alt_frequency = None
        if n_dna is not None:
            if record.samples.items()[n_dna][1].items()[2][1][0] is not None:
                self.normal_alt_depth = record.samples.items()[n_dna][1].items()[2][1][1]
            if record.samples.items()[n_dna][1].items()[1][0] is not None:
                self.normal_total_depth = record.samples.items()[n_dna][1].items()[1][1]
            if (self.normal_total_depth is not None) and (self.normal_alt_depth is not None):
                try:
                    self.normal_alt_frequency = self.normal_alt_depth / self.normal_total_depth
                except ZeroDivisionError:
                    self.normal_alt_frequency = 1.0
        self.rna_total_depth = None
        self.rna_alt_depth = None
        self.rna_alt_frequency = None
        if t_rna is not None:
            if record.samples.items()[t_rna][1].items()[2][1][0] is not None:
                self.rna_alt_depth = record.samples.items()[t_rna][1].items()[2][1][1]
            if record.samples.items()[t_rna][1].items()[1][0] is not None:
                self.rna_total_depth = record.samples.items()[t_rna][1].items()[1][1]
            if (self.rna_alt_depth is not None) and (self.rna_total_depth is not None):
                self.rna_alt_frequency = self.rna_alt_depth / self.rna_total_depth
        self.num_cases_seen = 0
        self.related_variants = []

        # Flags for searching
        self.in_cosmic = (len(self.cosmic_patients) > 0)
        self.has_related_variants = None
        self.highest_tier = None

    def sort_annotations(self, db):
        for vcf_annotation in self.vcf_annotations:
            vcf_annotation.assign_appris_data(db)
        self.vcf_annotations.sort(key=sort_key)
        self.gene_name = self.vcf_annotations[0].gene_name
        self.effects = self.vcf_annotations[0].effects
        self.impact = self.vcf_annotations[0].impact
        self.notation = self.vcf_annotations[0].protein_notation
        self.rank = self.vcf_annotations[0].rank
        if self.notation == '':
            self.notation = self.vcf_annotations[0].coding_notation

        pass

    def as_mongo_dict(self):
        mongo_dict = {'referenceId': self.reference_id, 'caseId': self.case_id, 'ids': self.ids, 'chrom': self.chrom,
                      'pos': self.pos, 'alt': self.alt, 'selected': self.selected, 'geneName': self.gene_name,
                      'effects': self.effects, 'reference': self.reference, 'impact': self.impact,
                      'notation': self.notation, 'callSet': self.call_set, 'type': self.type,
                      'cosmicPatients': self.cosmic_patients, 'tumorAltDepth': self.tumor_alt_depth,
                      'tumorTotalDepth': self.tumor_total_depth, 'tumorAltFrequency': self.tumor_alt_frequency,
                      'normalTotalDepth': self.normal_total_depth, 'normalAltDepth': self.normal_alt_depth,
                      'normalAltFrequency': self.normal_alt_frequency, 'rnaAltDepth': self.rna_alt_depth,
                      'rnaTotalDepth': self.rna_total_depth, 'rnaAltFrequency': self.rna_alt_frequency,
                      'filters': self.filters, 'vcfAnnotations': [], 'mdaAnnotation': self.mda_annotation,
                      'mdaAnnotated': self.mda_annotated, 'utswAnnotated': self.utsw_annotated,
                      'likelyArtifact': self.likely_artifact,
                      'numCasesSeen': self.num_cases_seen,
                      'exacAlleleFrequency': self.exac_allele_frequency,
                      'somaticStatus': self.somatic_status,
                      'gnomadPopmaxAlleleFrequency': self.gnomad_popmax_af,
                      'gnomadHomozygotes': self.gnomad_homozygotes,
                      'gnomadHg19Variant': self.gnomad_hg19_variant,
                      'gnomadLcr': self.gnomad_lcr,
                      'oldBuilds': self.old_builds, 'relatedVariants': self.related_variants,
                      'inCosmic': self.in_cosmic, 'hasRelatedVariants': self.has_related_variants,
                      'oncokbGeneName': self.oncokb_gene_name, 'oncokbVariantName': self.oncokb_variant_name,
                      'isOncokbVariant': self.is_oncokb_variant,
                      'repeatTypes': self.repeat_types, 'isRepeat': self.is_repeat,
                      'callsetInconsistent': self.callset_inconsistent,
                      'rank': self.rank, 'inClinvar': self.in_clinvar,
                      'maxCosmicPatients': self.max_cosmic_patients,
                      'highestTier': self.highest_tier,
                      }
        for annotation in self.vcf_annotations:
            mongo_dict['vcfAnnotations'].append(annotation.as_mongo_dict())

        mongo_dict['infoFields'] = self.info_dict
        return mongo_dict
