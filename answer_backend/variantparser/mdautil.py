import json
from variantparser.dbutil import VariantNotFoundError
from variantparser import annotation_to_oncokb_alteration
import uuid
import maya
import math


def annotate_case(db, mda_report):
    case_id = db.find_mda_case(mda_report)
    found = 0
    not_found = 0
    for key, annotation in mda_report['annotationRows'].items():
        try:
            mongo_variant = db.find_mda_variant(annotation, case_id)
            mongo_variant['mdaAnnotation'] = annotation
            mongo_variant['mdaAnnotated'] = True
            db.update_variant(mongo_variant)
            # print(mongo_variant)
            found += 1
        except VariantNotFoundError as error:
            not_found += 1
            print('Warning:', error.message)
    print("Found:", found, "variants and didn't find:", not_found, "variants.")
    db.add_trials(case_id, mda_report)


def annotation_to_moclia_alteration(annotation):
    alteration = None
    print(annotation['proteinNotation'])
    print(annotation['codingNotation'])
    if annotation['proteinNotation']:
        alteration = annotation['geneName'] + '_' + annotation_to_oncokb_alteration(
            annotation['proteinNotation'].split('.')[1])
    elif annotation['codingNotation']:
        alteration = annotation['geneName'] + '_' + annotation['codingNotation']
    return alteration


def get_copy_number(variant):
    cnv = variant.get('relatedCNV', None)
    if cnv:
        return cnv['copyNumber']
    else:
        return '2'


def variant_to_moclia(db, variant, case_id, utsw_id, accession_id):
    mda_transcript = db.get_mda_transcript(variant['geneName'])
    for annotation in variant['vcfAnnotations']:
        if annotation['featureId'].split('.')[0] == mda_transcript:
            return annotation_to_moclia(variant, annotation, utsw_id, accession_id)
    pass


def get_moclia_abberation(cnv):
    if cnv['copyNumber'] > 2:
        return "Amplification"
    else:
        return "Deletion"


def moclia_base_row(utsw_id, accession_id):
    array = []
    array.append("UTSW_" + utsw_id)  # 0 Patient ID
    array.append('UTSW')  # 1 Provider
    array.append('TruSight PanCancer')  # 2 Panel Name
    array.append(accession_id)  # 3 Report ID
    maya_time = maya.now()
    date = str(maya_time.month).zfill(2) + '/' + str(maya_time.day).zfill(2) + '/' + str(maya_time.year)
    array.append(date)  # 4 Report Date
    array.append('Final')  # 5 Result Status
    array.append('')  # 6 Gene Name
    array.append('')  # 7 Alteration
    array.append('')  # 8 Fusion Partner
    array.append('Tumor-based NGS')  # 9 Test Type
    array.append('')  # 10 Somatic
    array.append('')  # 11 Copy Number
    array.append('')  # 12 Allelic Frequency
    array.append('')  # 13 Transcript ID
    array.append('')  # 14 Nucleotide Change
    array.append('')  # 15 External Specimen ID
    array.append('')  # 16 Specimen Collect Date
    array.append('')  # 17 Path Tissue Site
    return array


def annotation_to_moclia(variant, annotation, utsw_id, accession_id):
    array = moclia_base_row(utsw_id, accession_id)
    array[6] = annotation['geneName']  # 6 Gene Name
    array[7] = annotation_to_moclia_alteration(annotation)  # 7 Alteration
    array[10] = variant['somaticStatus']  # 10 Somatic
    array[11] = get_copy_number(variant)  # 11 Copy Number
    array[12] = str(math.floor((100 * variant['tumorAltFrequency']) + 0.5))  # 12 Allelic Frequency
    array[14] = annotation['codingNotation']  # 14 Nucleotide Change
    return ",".join(array)


def translocation_to_moclia(db, translocation, utsw_id, accession_id):
    array = moclia_base_row(utsw_id, accession_id)
    array[6] = translocation['leftGene']  # 6 Gene Name
    array[7] = translocation['leftGene'] + translocation['rightGene']  # 7 Alteration
    array[8] = translocation['rightGene']  # 8 Fusion Partner


def create_cnv_row(cnv, gene, utsw_id, case_id, accession_id):
    array = moclia_base_row(utsw_id, accession_id)
    array[6] = gene  # 6 Gene Name
    array[7] = gene + "_" + get_moclia_abberation(cnv)  # 7 Alteration
    array[11] = str(cnv['copyNumber'])  # 11 Copy Number
    return ",".join(array)


def cnv_to_moclia(db, cnv, case_id, utsw_id, accession_id):
    # 1 Row per gene
    # Check if genes are in MDA list
    # copy number > 2 gain
    # copy number < 2 loss
    rows = []
    for gene in cnv['genes']:
        if db.is_mda_gene(gene):
            rows.append(create_cnv_row(cnv, gene, utsw_id, case_id, accession_id))
    return rows


def create_moclia_output(db, case_id):
    variants = db.get_selected(case_id, 'variants')
    output_array = []
    header_list = []
    header_list.append('Patient de-Identified ID')
    header_list.append('Provider')
    header_list.append('Panel Name')
    header_list.append('Report ID (Accession)')
    header_list.append('Report Date')
    header_list.append('Result Status')
    header_list.append('Gene')
    header_list.append('Alteration')
    header_list.append('Fusion Partner')
    header_list.append('Test Type')
    header_list.append('Somatic Germline')
    header_list.append('Copy Number Change')
    header_list.append('Allelic Frequency')
    header_list.append('Transcript ID')
    header_list.append('Nucleotide Change')
    header_list.append('External Specimen ID')
    header_list.append('Specimen Collect Date')
    header_list.append('Path Tissue Site')
    header = ','.join(header_list)
    output_array.append(header)
    accession_id = str(uuid.uuid4())
    try:
        utsw_id = db.db.cases.find_one({'caseId': case_id})['utswUuid']
    except KeyError:
        db.create_utsw_uuid(case_id)
        utsw_id = db.db.cases.find_one({'caseId': case_id})['utswUuid']
    for variant in variants:
        try:
            output_array.append(variant_to_moclia(db, variant, case_id, utsw_id, accession_id))
        except TypeError:
            pass
    translocations = db.get_selected(case_id, 'translocations')
    for translocation in translocations:
        left_gene = translocation['leftGene']
        right_gene = translocation['rightGene']
        if db.is_mda_gene(left_gene) and db.is_mda_gene(right_gene):
            output_array.append(translocation_to_moclia(db, translocation, case_id, utsw_id, accession_id))
        pass
    cnvs = db.get_selected(case_id, 'cnvs')
    for cnv in cnvs:
        output_array += cnv_to_moclia(db, cnv, case_id, utsw_id, accession_id)

    return "\n".join(output_array)
