import time
from clarityutils import clarityapi as clarity
from xml.dom.minidom import parseString
from Bio.SeqUtils import seq1
from .variant import Variant
from .fpkm import Fpkm
from .variant import CaseLoadingError
from .translocation import Translocation
from .virus import Virus
from .referencevariant import ReferenceVariant
from .referencetranslocation import ReferenceTranslocation
from .referencevirus import ReferenceVirus
from .copynumbervariation import CopyNumberVariation
from .referencecopynumbervariation import ReferenceCopyNumberVariation
from .ballelefrequency import BAlleleFrequency
from .mutationalsignature import MutationalSignature
from requests.exceptions import ProxyError
import tokensecret
import json
from pyliftover import LiftOver
import maya
import uuid
import requests
import logging
import sys

from operator import itemgetter
import pymongo

TUMOR_ACCESSIONING = 'Tumor Accessioning'
NORMAL_ACCESSIONING = 'Normal Accessioning'
try:
    import parserparameters

    LIFTOVER_LOCATION = parserparameters.LIFTOVER_LOCATION
except ImportError:
    LIFTOVER_LOCATION = '/home/answerbe/resources/hg38ToHg19.over.chain.gz'
logger = logging.getLogger("AnswerReceiver")

TYPE_TO_COLLECTION = {
    'snp': 'variants',
    'cnv': 'cnvs',
    'translocation': 'translocations',
    'virus': 'viruses',
}

AA_NAMES = {
    'Ala': 'A',
    'Arg': 'R',
    'Asn': 'N',
    'Asp': 'D',
    'Cys': 'C',
    'Glu': 'E',
    'Gln': 'Q',
    'Gly': 'G',
    'His': 'H',
    'Ile': 'I',
    'Leu': 'L',
    'Lys': 'K',
    'Met': 'M',
    'Phe': 'F',
    'Pro': 'P',
    'Ser': 'S',
    'Thr': 'T',
    'Trp': 'W',
    'Tyr': 'Y',
    'Val': 'V',
}

TIERS = {
    '1A': 0,
    '1B': 1,
    '2C': 2,
    '2D': 3,
    '3': 4,
    '4': 5,
    'Unknown': 10
}

STRING_TIERS = {
    0: '1A',
    1: '1B',
    2: '2C',
    3: '2D',
    4: '3',
    5: '4',
    10: 'Unknown'
}


def load_case_files(case, variant_file, translocation_df, cnv_file, cnr_file, cns_file, ballele_freq_df, tmb_df,
                    fpkm_df, virus_df, mutation_signature_df, tumor_sample_name, db):
    case.load_virus_df(virus_df)
    case.load_ballele_freq_df(ballele_freq_df)
    case.load_mutation_signature_df(mutation_signature_df)
    case.load_quality_metrics(tumor_sample_name)
    # case.load_tmb(tmb_file)
    case.load_tmb_df(tmb_df)
    case.load_variant_file(variant_file)
    case.load_fpkm_df(fpkm_df)
    hg38_to_19 = LiftOver(LIFTOVER_LOCATION)
    case.assign_variant_references(db, hg38_to_19)
    case.sort_variant_annotations(db)
    case.liftover_variants(hg38_to_19)
    case.find_related_variants(db)
    case.oncokb_annotate_variants(db)
    logger.info("Loading translocation file.")
    # case.load_translocation_file(translocation_file)
    case.load_translocation_df(translocation_df)
    logger.info("Assigning translocation references.")
    case.assign_translocation_references(db)
    logger.info("Assigning virus references.")
    case.assign_virus_references(db)

    if cnv_file is not None:
        case.load_cnv_file(cnv_file)
    case.assign_cnv_references(db)
    case.annotate_variant_copy_number(db)
    if cnr_file is not None and cns_file is not None:
        case.load_cnv_plots(cnr_file, cns_file)
    pass


def create_case_from_manifest(manifest, clarity_api, jlogger, db):
    jlogger.info("Attempting to run load_files.")
    manifest.load_files()
    jlogger.info("Manifest project_name: " + str(manifest.project_name))
    case = Case(manifest.project_name)
    try:
        case.load_metadata(clarity_api)
    except clarity.UdfNotFoundError as e:
        raise CaseLoadingError(e.name + ": " + e.message)
    except ProxyError as e:
        raise CaseLoadingError("Unable to connect to Clarity")
    load_case_files(case, manifest.variant_file, manifest.translocation_file, manifest.cnv_file, manifest.cnr_file,
                    manifest.cns_file, manifest.ballele_freq_file, manifest.tmb_file, manifest.fpkm_file,
                    manifest.virus_file, manifest.mutation_signature_file, manifest.tumor_sample_name, db)
    case.load_virus_df(manifest.virus_file)
    case.load_ballele_freq_df(manifest.ballele_freq_file)

    return case


def create_case(case_name, clarity_api, variant_file, translocation_df, cnv_file, cnr_file, cns_file,
                ballele_freq_df, tmb_df,
                fpkm_df, virus_df, mutation_signature_df, tumor_sample_name, db):
    case = Case(case_name)
    try:
        case.load_metadata(clarity_api)
    except clarity.UdfNotFoundError as e:
        raise CaseLoadingError(e.name + ": " + e.message)
    except ProxyError as e:
        raise CaseLoadingError("Unable to connect to Clarity")
    load_case_files(case, variant_file, translocation_df, cnv_file, cnr_file, cns_file, ballele_freq_df, tmb_df,
                    fpkm_df, virus_df, mutation_signature_df, tumor_sample_name, db)
    # case.load_virus_df(virus_df)
    # case.load_ballele_freq_df(ballele_freq_df)
    # case.load_mutation_signature_df(mutation_signature_df)
    # case.load_quality_metrics(tumor_sample_name)
    # # case.load_tmb(tmb_file)
    # case.load_tmb_df(tmb_df)
    # case.load_variant_file(variant_file)
    # case.load_fpkm_df(fpkm_df)
    # hg38_to_19 = LiftOver(LIFTOVER_LOCATION)
    # case.assign_variant_references(db, hg38_to_19)
    # case.sort_variant_annotations(db)
    # case.liftover_variants(hg38_to_19)
    # case.find_related_variants(db)
    # case.oncokb_annotate_variants(db)
    # logger.info("Loading translocation file.")
    # # case.load_translocation_file(translocation_file)
    # case.load_translocation_df(translocation_df)
    # logger.info("Assigning translocation references.")
    # case.assign_translocation_references(db)
    # logger.info("Assigning virus references.")
    # case.assign_virus_references(db)
    #
    # if cnv_file is not None:
    #     case.load_cnv_file(cnv_file)
    # case.assign_cnv_references(db)
    # case.annotate_variant_copy_number(db)
    # if cnr_file is not None and cns_file is not None:
    #     case.load_cnv_plots(cnr_file, cns_file)
    case.check_annotations(db)
    return case


def get_sample_received_date(case_name, api, project):
    process_list_xml = api.GET(api.base_uri + 'processes/?projectname=' + case_name)
    process_list_dom = parseString(process_list_xml)
    nodes = process_list_dom.getElementsByTagName('process')
    tumor_date = time.gmtime(0)
    normal_date = time.gmtime(0)

    for node in nodes:
        limsid = node.getAttribute("limsid")
        process = clarity.Process(api.GET(api.base_uri + 'processes/' + limsid))
        if process.type_name == TUMOR_ACCESSIONING:
            tumor_date = time.strptime(process.date_run, '%Y-%m-%d')
        elif process.type_name == NORMAL_ACCESSIONING:
            normal_date = time.strptime(process.date_run, '%Y-%m-%d')
    accessioning_date = max(normal_date, tumor_date)
    start_date = time.strptime(get_udf(project, 'Started', '1970-01-01'), '%Y-%m-%d')

    return time.strftime('%Y-%m-%d', max(accessioning_date, start_date))


def type_to_collection(type):
    return TYPE_TO_COLLECTION[type]


def get_project(case_id, clarity_api):
    project_query_xml = clarity_api.GET(clarity_api.base_uri + 'projects/?name=' + case_id)
    project_query_dom = parseString(project_query_xml)
    project_limsid_nodes = project_query_dom.getElementsByTagName('project')

    if len(project_limsid_nodes) == 0:
        raise CaseLoadingError("Project not found in LIMS")
    if len(project_limsid_nodes) > 1:
        raise CaseLoadingError("Multiple projects with that name found")

    project_limsid = project_limsid_nodes[0].getAttribute('limsid')
    logger.info(str(case_id) + " " + "Found project limsid " + str(project_limsid))
    # print(maya.now().iso8601(), case_id, "Found project limsid", project_limsid)
    case = clarity.Project(clarity_api.GET(clarity_api.base_uri + 'projects/' + project_limsid))
    return case


# maximum cleverness, minimum readability
def get_related_variants(variant_list: list, idx: int, related_variants: list, increment: int,
                         original_variant: object) -> list:
    if idx < 0:
        return related_variants
    if idx >= len(variant_list):
        return related_variants
    if original_variant.chrom != variant_list[idx].chrom:
        return related_variants
    if increment == -1:
        if variant_list[idx].pos + len(variant_list[idx].alt) >= original_variant.pos:
            to_append = variant_list[idx].as_mongo_dict()
            to_append.pop('relatedVariants')
            to_append.pop('infoFields')
            related_variants.append(to_append)
    else:
        if original_variant.pos + len(original_variant.alt) >= variant_list[idx].pos:
            related_variants.append(variant_list[idx].as_mongo_dict())

    return get_related_variants(variant_list, idx + increment, related_variants, increment, original_variant)


def get_oncokb_gene_name(variant, ref_db):
    query = {'hugoSymbol': variant.gene_name}
    cursor = ref_db.oncokb_genes.find(query)
    if cursor.count() == 1:
        return cursor[0]['hugoSymbol']
    query = {'geneAliases': {'$in': [variant.gene_name]}}
    cursor = ref_db.oncokb_genes.find(query)
    if cursor.count() == 1:
        return cursor[0]['hugoSymbol']
    return None


def annotation_to_oncokb_alteration(alteration):
    if len(alteration) < 4:
        return ''
    else:
        for key, value in AA_NAMES.items():
            alteration = alteration.replace(key, value)
        return alteration


def get_oncokb_variant_name(variant, ref_db):
    if variant.oncokb_gene_name is not None:
        for annotation in variant.vcf_annotations:
            query_alteration = annotation_to_oncokb_alteration(annotation.protein_notation[2:])
            query = {'gene.hugoSymbol': variant.oncokb_gene_name, 'alteration': query_alteration}
            # print('Alteration in:', annotation.protein_notation, 'Alteration out:', query_alteration)
            cursor = ref_db.oncokb_variants.find(query)
            if cursor.count() == 1:
                # print('Found OncoKB alteration in gene:', variant.oncokb_gene_name, 'alteration', query_alteration)
                variant.is_oncokb_variant = True
                return cursor[0]['alteration']
            else:
                return None
    else:
        return None
    pass


def get_udf(object, udf_name, default):
    try:
        udf = object.get_udf(udf_name).value
    except clarity.UdfNotFoundError:
        udf = default
    return udf


class Case(object):
    def __init__(self, case_id):
        self.case_name = case_id
        # Website information
        self.active = True
        self.user = None
        # Patient demographics
        self.case_id = None
        self.tumor_tissue_type = None
        self.normal_tissue_type = None
        self.tumor_percent = None
        self.epic_order_number = None
        self.normal_id = None
        self.tumor_id = None
        self.tumor_block = None
        self.patient_name = None
        self.date_of_birth = None
        self.gender = None
        self.icd_10 = None
        self.epic_order_date = None
        self.received_date = None
        self.tumor_collection_date = None
        self.ordering_physician = None
        self.authorizing_physician = None
        self.medical_record_number = None
        self.institution = None
        self.type = None
        self.assigned_to = []
        self.oncotree_diagnosis = None
        # Variants
        self.variants = []
        self.translocations = []
        self.fpkm = []
        self.ballele_freq = []
        self.copy_number_variations = []
        self.cnr = []
        self.cns = []
        self.viruses = []
        self.mutational_signatures = []
        self.normal_dna_bam_path = None
        self.tumor_dna_bam_path = None
        self.tumor_rna_bam_path = None
        self.musica_vcf_path = None
        self.mutation_signature_image_path = None
        self.utsw_uuid = None
        self.tmb = None
        self.tmb_class = None
        self.msi = None
        self.msi_class = None
        self.dedup_average_depth = None
        self.dedup_percent_over_100x = None
        self.group_ids = []
        self.test_name = None
        self.tumor_panel = None
        self.report_order_number = None
        self.report_accession_id = None

    def annotate_variant_copy_number(self, db):
        pass

    def get_tumor_percentage(self, case, api):
        # tumor accessioning process ID 24-9411
        # test project: 318321181-92384513
        get_string = api.base_uri + 'processes/?projectname=' + case.name + '&type=Tumor%20Macro%20Dissection'
        process_xml = api.GET(get_string)
        processes_dom = parseString(process_xml)
        if len(processes_dom.getElementsByTagName('process')) == 0:
            return None
        process_limsid = processes_dom.getElementsByTagName('process')[0].getAttribute('limsid')
        process = clarity.Process(api.GET(api.base_uri + 'processes/' + process_limsid))
        for input_output_map in process.input_output_maps:
            if input_output_map.output_type == 'Analyte':
                output_limsid = input_output_map.output_limsid
        tumor_artifact = clarity.Artifact(api.GET(api.base_uri + 'artifacts/' + output_limsid))
        try:
            tumor_percent = tumor_artifact.get_udf('Percent Tumor').value
        except clarity.UdfNotFoundError:
            logger.warn("No Percent Tumor found.")
            # print("Warning: No Percent Tumor found.")
            tumor_percent = None
        return tumor_percent

    def load_quality_metrics(self, tumor_sample_name):
        base_url = 'https://nuclia.biohpc.swmed.edu/NuCLIAVault/getTumorCoverageFromAPI'
        params = {'token': tokensecret.nuclia_token, 'sampleLabId': tumor_sample_name}
        response = requests.get(base_url, params)
        logger.debug(str(response.url))
        logger.debug(str(response.text))
        # print(response.url)
        # print(response.text)
        try:
            data = response.json()
        except json.decoder.JSONDecodeError:
            return
        try:
            self.dedup_average_depth = data['rawAvgDepth']
            self.dedup_percent_over_100x = data['rawPctOver100X']
        except KeyError:
            return
        pass

    def load_metadata(self, clarity_api):
        project = get_project(self.case_name, clarity_api)
        self.case_id = project.limsid
        self.tumor_tissue_type = project.get_udf('Tumor tissue type').value
        self.normal_tissue_type = project.get_udf('Normal tissue type').value
        self.epic_order_number = project.get_udf('Epic order number').value
        self.normal_id = self.get_normal_id(project)
        self.tumor_id = self.get_tumor_sample_id(project, clarity_api)
        try:
            self.tumor_block = project.get_udf('CoPath block number').value
        except clarity.UdfNotFoundError:
            self.tumor_block = "Not Found"
        self.patient_name = project.get_udf('Patient name').value
        self.date_of_birth = project.get_udf('Date of birth').value
        self.gender = project.get_udf('Gender').value
        self.icd_10 = get_udf(project, 'ICD10', 'Unknown')
        self.epic_order_date = project.get_udf('Epic order date').value
        self.received_date = get_sample_received_date(project.name, clarity_api, project)
        self.tumor_collection_date = project.get_udf('Tumor collection date').value
        self.ordering_physician = project.get_udf('Ordering provider').value
        self.authorizing_physician = project.get_udf('Authorizing provider').value
        self.medical_record_number = project.get_udf('Medical record number').value
        self.institution = project.get_udf('Referring institution').value
        self.type = project.get_udf('Clinical/Research').value
        self.oncotree_diagnosis = get_udf(project, 'OncoTree Diagnosis', None)
        self.utsw_uuid = str(uuid.uuid4())
        self.tumor_percent = self.get_tumor_percentage(project, clarity_api)
        self.test_name = project.get_udf('Comments').value
        self.lab_notes = get_udf(project, 'Case notes', '')
        self.tumor_panel = get_udf(project, 'Type of Cancer', None)
        self.group_ids.append("1")
        self.report_order_number = project.get_udf('Report order number').value
        self.report_accession_id = project.get_udf('Report accession ID').value

    def load_virus_df(self, virus_df):
        for _, row in virus_df.iterrows():
            self.viruses.append(Virus(row, self.case_id))
        pass

    def load_translocation_file(self, translocation_file):
        # Header: FusionName      LeftGene        LefttBreakpoint LeftGeneExons   LeftStrand      RightGene
        # RightBreakpoint RightGeneExons  RightStrand RNAReads DNAReads        FusionType      Annot
        for line in translocation_file:
            self.translocations.append(Translocation(line, self.case_id))

    def load_translocation_df(self, translocation_df):
        for _, row in translocation_df.iterrows():
            self.translocations.append(Translocation(row, self.case_id))

    def load_cnv_file(self, cnv_file):
        header = cnv_file.readline()
        try:
            last_line = cnv_file.readline()
        except IOError:
            return
        if last_line == '':
            return
        genes = []
        array = last_line.strip().split('\t')
        genes.append(array[0])
        for line in cnv_file:
            last_array = last_line.strip().split('\t')
            array = line.strip().split('\t')
            if (array[1] == last_array[1]) and (array[2] == last_array[2]) and array[3] == last_array[3]:
                genes.append(array[0])
            else:
                self.copy_number_variations.append(CopyNumberVariation(genes, last_line, self.case_id))
                genes = []
                genes.append(array[0])
            last_line = line
        self.copy_number_variations.append(CopyNumberVariation(genes, last_line, self.case_id))

    def load_tmb_df(self, tmb_df):
        for index, row in tmb_df.iterrows():
            if row.get('Metric') == 'TMB':
                self.tmb = row.get('Value')
                self.tmb_class = row.get('Class')
            elif row.get('Metric') == 'MSI':
                self.msi = row.get('Value')
                self.msi_class = row.get('Class')
            elif row.get('Metric') is None:
                self.tmb = row.get('TMB')

    def load_tmb(self, tmb_file):
        if tmb_file is None:
            return
        header = tmb_file.readline()
        self.tmb = tmb_file.readline().strip().split(",")[1]
        pass

    def load_fpkm_df(self, fpkm_df):
        if fpkm_df is not None:
            for row_index, row in fpkm_df.iterrows():
                fpkm_row = Fpkm(row, self.case_id, self.oncotree_diagnosis)
                self.fpkm.append(fpkm_row)
        pass

    def load_mutation_signature_df(self, mutation_signature_df):
        if mutation_signature_df is not None:
            for row_index, row in mutation_signature_df.iterrows():
                mutation_signature_row = MutationalSignature(row, self.case_id)
                self.mutational_signatures.append(mutation_signature_row)

    def load_ballele_freq_df(self, ballele_freq_df):
        if ballele_freq_df is not None:
            for row_index, row in ballele_freq_df.iterrows():
                ballele_freq_row = BAlleleFrequency(row, self.case_id)
                self.ballele_freq.append(ballele_freq_row)
        pass

    def load_variant_file(self, variant_file):
        for record in variant_file:
            if record.alts[0] != '<DUP>':
                self.variants.append(Variant(record, self.case_id))

    def liftover_variants(self, hg38_to_19):
        for variant in self.variants:
            hg19_position_array = hg38_to_19.convert_coordinate(variant.chrom, variant.pos, '+')
            if len(hg19_position_array) > 1:
                logger.info(str(hg19_position_array))
            try:
                final_position = hg19_position_array[0]
                variant.old_builds['hg19'] = {
                    'pos': final_position[1],
                    'chrom': final_position[0]
                }
            except IndexError:
                pass
        pass

    def find_related_variants(self, db):
        logger.info("Finding related variants.")
        self.variants.sort(key=lambda x: (x.chrom, x.pos))
        i = 0

        sys.setrecursionlimit(400000)
        while i < len(self.variants):
            related_variants = []
            # print(self.variants[i].as_mongo_dict())
            get_related_variants(self.variants, i - 1, related_variants, -1, self.variants[i])
            get_related_variants(self.variants, i + 1, related_variants, 1, self.variants[i])
            self.variants[i].related_variants = related_variants
            self.variants[i].has_related_variants = (len(related_variants) > 0)
            i += 1

    def assign_virus_references(self, db):
        for virus in self.viruses:
            reference_virus = db.get_reference_virus(virus)
            if reference_virus is None:
                reference_virus = ReferenceVirus.from_virus(virus)
                reference_virus.mongo_id = db.create_reference_virus(reference_virus)
            reference_virus.assign_case(self.case_id, self.type, db.db)
            virus.reference_id = reference_virus.mongo_id
            virus.num_cases_seen = len(reference_virus.clinical_cases)

    def assign_translocation_references(self, db):
        for translocation in self.translocations:
            reference_translocation = db.get_reference_translocation(translocation)
            if reference_translocation is None:
                reference_translocation = ReferenceTranslocation.from_translocation(translocation)
                reference_translocation.mongo_id = db.create_reference_translocation(reference_translocation)
            reference_translocation.assign_case(self.case_id, self.type, db.db)
            translocation.reference_id = reference_translocation.mongo_id
            translocation.num_cases_seen = len(reference_translocation.clinical_cases)

    def assign_cnv_references(self, db):
        for cnv in self.copy_number_variations:
            reference_cnv = db.get_reference_cnv(cnv)
            if reference_cnv is None:
                reference_cnv = ReferenceCopyNumberVariation.from_copy_number_variation(cnv)
                reference_cnv.mongo_id = db.create_reference_cnv(reference_cnv)
            reference_cnv.assign_case(self.case_id, self.type, db.db)
            cnv.reference_id = reference_cnv.mongo_id
            cnv.num_cases_seen = len(reference_cnv.clinical_cases)

    def load_cnv_plots(self, cnr_file, cns_file):
        for line in cnr_file:
            self.cnr.append(line.strip().split('\t'))
        for line in cns_file:
            self.cns.append(line.strip().split('\t'))
        pass

    def assign_variant_references(self, db, hg38_to_19):
        for variant in self.variants:
            reference_variant = db.get_reference_variant(variant)
            if reference_variant is None:
                reference_variant = ReferenceVariant.from_variant(variant)
                reference_variant.set_hg19(hg38_to_19)
                reference_variant.mongo_id = db.create_reference_variant(reference_variant)
            reference_variant.assign_case(self.case_id, self.type, db.db)

            variant.reference_id = reference_variant.mongo_id
            variant.num_cases_seen = len(reference_variant.clinical_cases)

    def sort_variant_annotations(self, db):
        for variant in self.variants:
            variant.sort_annotations(db)

    def oncokb_annotate_variants(self, db):
        logger.info("Annotating variants with Oncokb.")
        ref_db = db.client.reference
        for variant in self.variants:
            variant.oncokb_gene_name = get_oncokb_gene_name(variant, ref_db)
            variant.oncokb_variant_name = get_oncokb_variant_name(variant, ref_db)

    def check_annotations(self, db):
        ARTIFACT_CATEGORY_NAME = 'Likely Artifact'
        for variant in self.variants:
            variant_min_tier = 10
            utsw_annotations = db.get_annotation_list('snp', self.case_id, [variant.gene_name],
                                                      variant.reference_id,
                                                      self.oncotree_diagnosis)
            if len(utsw_annotations) > 0:
                variant.utsw_annotated = True
            for annotation in utsw_annotations:
                try:
                    if TIERS[annotation['tier']] < variant_min_tier:
                        variant_min_tier = TIERS[annotation['tier']]
                    if annotation['category'] == ARTIFACT_CATEGORY_NAME:
                        variant.likely_artifact = True
                except KeyError:
                    pass
            variant.highest_tier = STRING_TIERS[variant_min_tier]
        for cnv in self.copy_number_variations:
            cnv_min_tier = 10
            utsw_annotations = db.get_annotation_list('cnv', self.case_id, cnv.genes, cnv.reference_id,
                                                      self.oncotree_diagnosis)
            if len(utsw_annotations) > 0:
                cnv.utsw_annotated = True
            for annotation in utsw_annotations:
                try:
                    if TIERS[annotation['tier']] < cnv_min_tier:
                        cnv_min_tier = TIERS[annotation['tier']]
                    if annotation['category'] == ARTIFACT_CATEGORY_NAME:
                        cnv.likely_artifact = True
                except KeyError:
                    pass
            cnv.highest_tier = STRING_TIERS[cnv_min_tier]

        for translocation in self.translocations:
            translocation_min_tier = 10
            utsw_annotations = db.get_annotation_list('translocation', self.case_id, translocation.gene_names,
                                                      translocation.reference_id, self.oncotree_diagnosis)
            if len(utsw_annotations) > 0:
                translocation.utsw_annotated = True
            for annotation in utsw_annotations:
                try:
                    if TIERS[annotation['tier']] < translocation_min_tier:
                        translocation_min_tier = TIERS[annotation['tier']]
                    if annotation['category'] == ARTIFACT_CATEGORY_NAME:
                        translocation.likely_artifact = True
                except KeyError:
                    pass
            translocation.highest_tier = STRING_TIERS[translocation_min_tier]
            pass

    @staticmethod
    def get_normal_id(case):
        normal_id = ''
        backup_id = get_udf(case, "Normal label", None)
        if backup_id is not None:
            return backup_id
        try:
            if case.get_udf('Normal tissue type').value.strip().lower() == 'blood':
                normal_id = case.get_udf('Sunquest blood accession ID').value
            else:
                normal_id = case.get_udf('Sunquest saliva accession ID').value
        except clarity.UdfNotFoundError:
            logger.warn("No normal ID found.")
        return normal_id

    @staticmethod
    def get_tumor_sample_id(case, api):
        sample_limsids = case.get_samples(api)
        backup_id = get_udf(case, "Tumor label", None)
        if backup_id is not None:
            return backup_id
        for sample_limsid in sample_limsids:
            artifact = clarity.Artifact(api.GET(api.base_uri + 'artifacts/' + sample_limsid + 'PA1'))
            if artifact.name.endswith('_T'):
                container = clarity.Container(api.GET(api.base_uri + 'containers/' + artifact.container_limsid))
                if container.name == container.limsid:
                    return ''
                else:
                    return container.name
        return ''

    def as_mongo_dict(self):
        # Does not include variants, translocations, cnvs
        mongo_dict = {'user': self.user, 'active': self.active, 'caseName': self.case_name, 'caseId': self.case_id,
                      'tumorTissueType': self.tumor_tissue_type, 'normalTissueType': self.normal_tissue_type,
                      'epicOrderNumber': self.epic_order_number, 'epicOrderDate': self.epic_order_date,
                      'receivedDate': self.received_date, 'normalId': self.normal_id, 'tumorId': self.tumor_id,
                      'tumorBlock': self.tumor_block, 'patientName': self.patient_name,
                      'dateOfBirth': self.date_of_birth, 'gender': self.gender, 'icd10': self.icd_10,
                      'tumorCollectionDate': self.tumor_collection_date, 'orderingPhysician': self.ordering_physician,
                      'authorizingPhysician': self.authorizing_physician,
                      'medicalRecordNumber': self.medical_record_number, 'institution': self.institution,
                      'assignedTo': self.assigned_to, 'type': self.type,
                      'oncotreeDiagnosis': self.oncotree_diagnosis,
                      'tumorBam': self.tumor_dna_bam_path, 'normalBam': self.normal_dna_bam_path,
                      'rnaBam': self.tumor_rna_bam_path,
                      'utswUuid': self.utsw_uuid,
                      'tumorPercent': self.tumor_percent,
                      'tmb': self.tmb,
                      'tmbClass': self.tmb_class,
                      'msi': self.msi,
                      'msiClass': self.msi_class,
                      'dedupAvgDepth': self.dedup_average_depth,
                      'dedupPctOver100X': self.dedup_percent_over_100x,
                      'labNotes': self.lab_notes,
                      'testName': self.test_name,
                      'groupIds': self.group_ids,
                      'tumorVcf': self.musica_vcf_path,
                      'mutationalSignatureImage': self.mutation_signature_image_path,
                      'tumorPanel': self.tumor_panel,
                      'reportOrderNumber': self.report_order_number,
                      'reportAccessionId': self.report_accession_id,
                      }
        return mongo_dict
