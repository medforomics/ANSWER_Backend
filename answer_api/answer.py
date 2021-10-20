from flask import Flask
from flask_basicauth import BasicAuth
from flask import request
from flask import jsonify
from pymongo import MongoClient
from variantparser import dbutil as mongo
from bson.json_util import dumps, loads
from answerapi import InvalidParameters
from variantparser import create_moclia_output
from variantparser import get_trial_metadata
import apisecret
import json
import os

DEBUG = True

app = Flask(__name__)

app.config['BASIC_AUTH_USERNAME'] = apisecret.api_user
app.config['BASIC_AUTH_PASSWORD'] = apisecret.api_password
app.config['BASIC_AUTH_FORCE'] = True
test_var = "Hello, World! v1.0.0"
db_name = os.environ.get('ANSWER_DB_NAME','answer')
db = mongo.DbClient(db_name)
basic_auth = BasicAuth(app)


@app.route('/')
def hello_world():
    return test_var


@app.route('/cases/', methods=['GET'])
@app.route('/cases', methods=['GET'])
def display_cases():
    query = {}
    output = db.read('cases', query)
    return dumps(output, sort_keys=True, indent=4)


@app.route('/export/', methods=['GET'])
def export():
    output = ""
    return dumps(output)


@app.route('/case/<case_name>', methods=['GET'])
def display_case(case_name):
    query = {'caseId': case_name}
    db_filter = {'vcfAnnotations': 0, 'referenceId': 0, 'infoFields': 0, 'mdaAnnotation': 0, 'annotation': 0,
                 'relatedVariants': 0}
    variants = db.read('variants', query, db_filter)
    cnvs = db.read('cnvs', query, db_filter)
    translocations = db.read('translocations', query, db_filter)
    viruses = db.read('viruses', query, db_filter)
    if not variants:
        raise InvalidParameters('No variants found for this case ID.', status_code=400)
    if not cnvs:
        cnvs = []
    if not translocations:
        translocations = []
    if not viruses:
        viruses = []
    case = db.read('cases', query)[0]
    case['variants'] = variants
    case['cnvs'] = cnvs
    case['translocations'] = translocations
    case['viruses'] = viruses
    case['totalCases'] = db.count_cases()
    return dumps(case)


@app.route('/case/<case_name>/cnv', methods=['POST'])
def create_cnv(case_name):
    new_cnv = request.get_json()
    print(dumps(new_cnv, indent=2))
    return dumps(db.create_cnv(case_name, new_cnv))

### added by G
@app.route('/case/<case_name>/translocation', methods=['POST'])
def create_translocation(case_name):
    new_ftl = request.get_json()
    print(dumps(new_ftl, indent=2))
    return dumps(db.create_ftl(case_name, new_ftl))
###

@app.route('/case/<case_name>/mutsigs', methods=['GET'])
def get_mutational_signatures(case_name):
    query = {'caseId': case_name}
    signatures = db.read('mutational_signatures', query)
    return dumps({'message': 'Musica Mutational Signatures.',
                  'payload': signatures,
                  'success': True})


@app.route('/case/<case_name>/fpkm/<gene_name>', methods=['GET'])
def get_gene_fpkm(case_name, gene_name):
    query = {'caseId': case_name}
    projection = {'annotation': 0, 'clinicalTrials': 0}
    try:
        case = db.read('cases', query, projection)[0]
    except IndexError:
        return dumps({'message': 'Unable to find caseId',
                      'payload': None,
                      'success': False})

    oncotree_diagnosis = case['oncotreeDiagnosis']
    tissue_codes = db.get_tissue_oncotree_codes(oncotree_diagnosis)
    print("Outputting FPKM for gene", gene_name, "and case", case_name, "with Oncotree code", oncotree_diagnosis)
    db_filter = {'caseId': 1, 'oncotreeDiagnosis': 1, 'geneName': 1, 'fpkm': 1}

    fpkm_array = db.read('fpkm', {'geneName': gene_name, 'oncotreeDiagnosis': {'$in': tissue_codes}},
                         db_filter)
    return dumps({'message': 'Returned FPKM Values',
                  'payload': fpkm_array,
                  'success': True})

# Added by G        
@app.route('/case/<case_name>/tmb', methods=['GET'])
def get_tmb(case_name):
    query = {'caseId': case_name}
    projection = {'annotation': 0, 'clinicalTrials': 0}
    try:
        case = db.read('cases', query, projection)[0]
    except IndexError:
        return dumps({'message': 'Unable to find caseId',
                      'payload': None,
                      'success': False})

    oncotree_diagnosis = case['oncotreeDiagnosis']
    tissue_codes = db.get_tissue_oncotree_codes(oncotree_diagnosis)
    db_filter = {'_id': 0, 'caseId': 1, 'tmb': 1, 'oncotreeDiagnosis': 1}

    tmb_array = db.read('cases', {'tmb': {"$exists" : True}, 'oncotreeDiagnosis': {'$in': tissue_codes}},
                         db_filter)
    return dumps({'message': 'Returned tmb_array Values',
                  'payload': tmb_array,
                  'success': True})
#############

@app.route('/case/<case_name>/ballelefreq', methods=['GET'])
def get_ballele(case_name):
    query = {'caseId': case_name}
    output = db.read('ballelefreqs', query)
    return dumps({'message': 'Returning all Ballele frequency values.',
                  'payload': output,
                  'success': True})


@app.route('/case/<case_name>/summary', methods=['GET', 'POST'])
def get_file_paths(case_name):
    if request.method == 'GET':
        query = {'caseId': case_name}
        projection = {'annotation': 0, 'clinicalTrials': 0}
        output = db.read('cases', query, projection)[0]
        return dumps(output)
    elif request.method == 'POST':
        case = request.get_json()
        if DEBUG:
            print(dumps(case, indent=2))
        return dumps(db.update_case_details(case, case_name))
        # diagnosis = case.get('oncotreeDiagnosis', None)
        # if diagnosis is not None:
        #     return dumps(db.update_diagnosis(case))

    else:
        return 'Should never see this'


@app.route('/case/<case_name>/cnvplot', methods=['GET'])
def get_cnv_plot(case_name):
    query = {'caseId': case_name}
    output = db.read('cnv_plot', query, None)[0]
    ballelefreqs = db.read('ballelefreqs', query)
    output['ballelefreqs'] = ballelefreqs
    return dumps(output)


@app.route('/case/<case_name>/trials', methods=['GET'])
def get_clinicial_trials(case_name):
    query = {'caseId': case_name}
    output = db.read('cases', query, None)[0]
    print(output.get('clinicalTrials', {}))
    return dumps(output.get('clinicalTrials', {}))


@app.route('/case/<case_name>/itd', methods=['POST'])
def create_itd(case_name):
    print("Incoming ITD Request")
    gene_name = request.get_json()['geneName']
    new_cnv = {
        'genes': [gene_name],
        'copyNumber': 3,
        'aberrationType': 'ITD',
    }
    print(dumps(new_cnv, indent=2))
    return dumps(db.create_cnv(case_name, new_cnv))


@app.route('/case/<case_name>/filter', methods=['GET', 'POST'])
def filter_variants(case_name):
    snp_query = {'caseId': case_name}
    cnv_query = {'caseId': case_name}
    ftl_query = {'caseId': case_name}
    virus_query = {'caseId': case_name}
    query_filters = request.get_json()
    print(json.dumps(query_filters, indent=2, ))
    for query_filter in query_filters['filters']:
        if query_filter['type'] == 'snp':
            if query_filter['field'] == 'annotations':
                if query_filter['valueTrue'] and query_filter['valueFalse']:
                    pass
                elif query_filter['valueTrue']:
                    snp_query['$or'] = [{'utswAnnotated': True}, {'mdaAnnotated': True}]
                elif query_filter['valueFalse']:
                    snp_query['utswAnnotated'] = False
                    snp_query['mdaAnnotated'] = False
            else:
                generated_query, custom_field = mongo.generate_query_filter(query_filter)
                if generated_query:
                    if custom_field != None:
                        snp_query[custom_field] = generated_query[custom_field]
                    else:
                        snp_query[query_filter['field']] = generated_query
        elif query_filter['type'] == 'cnv':
            if query_filter['field'] == 'cnvGeneName':
                query_filter['field'] = 'genes'
            if query_filter['field'] == 'cnvCopyNumber':
                query_filter['field'] = 'copyNumber'
            generated_query, custom_field = mongo.generate_query_filter(query_filter)
            cnv_query[query_filter['field']] = generated_query

        elif query_filter['type'] == 'ftl':
            generated_query, custom_field = mongo.generate_query_filter(query_filter)
            ftl_query[query_filter['field']] = generated_query
    if DEBUG:
        print(snp_query)
    db_filter = {'vcfAnnotations': 0, 'referenceId': 0, 'infoFields': 0, 'mdaAnnotation': 0, 'annotation': 0,
                 'relatedVariants': 0}
    case_query = {'caseId': case_name}
    variant_array = db.read('variants', snp_query, db_filter)
    cnvs = db.read('cnvs', cnv_query, db_filter)
    translocations = db.read('translocations', ftl_query, db_filter)
    viruses = db.read('viruses', virus_query, db_filter)
    case = db.read('cases', case_query)[0]
    case['variants'] = variant_array
    case['cnvs'] = cnvs
    case['translocations'] = translocations
    case['viruses'] = viruses
    case['totalCases'] = db.count_cases()
    return dumps(case)


@app.route('/case/<case_name>/metadata', methods=['GET'])
def refresh_metadata(case_name):
    return "Sure"


@app.route('/case/<case_name>/setOwner', methods=['POST'])
def set_case_owner(case_name):
    request_object = request.get_json()
    print("Updating owner for case", case_name)
    print(dumps(request_object))
    user_id = request_object.get('userId', None)
    return dumps(db.set_case_owner(case_name, user_id))


@app.route('/case/<case_name>/assignusers', methods=['PUT'])
def assign_case(case_name):
    user_ids = request.args.get('userIds', None)
    if user_ids is None:
        raise InvalidParameters('list of userIds is invalid or missing.', status_code=400)
    elif user_ids == '':
        user_ids = []
    else:
        user_ids = user_ids.split(',')
    if case_name is None:
        raise InvalidParameters('caseId is invalid or missing.', status_code=400)
    output = db.update_assigned_users(case_name, user_ids)
    if output is None:
        raise InvalidParameters('caseId not found', status_code=400)
    return dumps(output, sort_keys=True, indent=4)


@app.route('/case/<case_name>/assigngroups', methods=['PUT'])
def assign_groups(case_name):
    group_ids = request.args.get('groupIds', None)
    if group_ids is None:
        raise InvalidParameters('List of groupIds is invalid or missing.', status_code=400)
    elif group_ids == '':
        group_ids = []
    else:
        group_ids = group_ids.split(',')
    if case_name is None:
        raise InvalidParameters('caseId is invalid or missing.', status_code=400)
    output = db.update_assigned_groups(case_name, group_ids)
    if output is None:
        raise InvalidParameters('caseId not found', status_code=400)
    return dumps(output)


@app.route('/case/<case_name>/updateHistory/<step_number>')
def update_case_history(case_name, step_number):
    updated_case = db.update_case_history(case_name, step_number)
    return dumps(updated_case)


@app.route('/case/<case_name>/sendToEpic', methods=['POST'])
def send_to_epic(case_name):
    updated_case = db.update_case_history(case_name, 5)
    return dumps(updated_case)


@app.route('/case/<case_name>/load', methods=['POST'])
def timestamp_loading(case_name):
    updated_case = db.update_case_history(case_name, 0)
    return dumps(updated_case)


@app.route('/case/<case_name>/getselectedbyname')
def get_selected_variants(case_name):
    # This case_name is not the ORD number but the order#-mrn
    query = {'caseName': case_name}
    try:
        case = db.read('cases', query)[0]
    except IndexError:
        return "Case name not found."

    return dumps(db.get_all_selected(case))


@app.route('/case/<case_name>/review', methods=['POST'])
def update_history(case_name):
    updated_case = db.update_case_history(case_name, 2)
    return dumps(updated_case)


@app.route('/case/<case_name>/report', methods=['POST'])
def start_report(case_name):
    updated_case = db.update_case_history(case_name, 3)
    return dumps(updated_case)


@app.route('/case/<case_name>/reports', methods=['GET'])
def get_reports(case_name):
    return dumps({'result': db.get_reports(case_name)})


@app.route('/allreports', methods=['GET'])
def get_all_reports():
    # TODO: Implement
    # Return a dictionary of all cases with a list of reports inside each one
    # { 'ORD###' : [report, report]}
    query = {}
    cases = db.read('cases', query)
    output = {}
    for case in cases:
        output[case['caseId']] = db.get_reports(case['caseId'])
    return dumps({'message': 'Returning all reports.',
                  'payload': output,
                  'success': True})


@app.route('/case/<case_name>/savereport', methods=['POST', 'PUT'])
def save_report(case_name):
    report = loads(request.get_data())
    if request.method == 'POST':
        modified_report = db.create_report(case_name, report)
    if request.method == 'PUT':
        modified_report = db.update_report(case_name, report)
    return dumps(modified_report)


@app.route('/searchannotations/', methods=['POST'])
def search_annotations():
    search_query = request.get_json()
    print(dumps(search_query, indent=2))
    type = search_query['type']
    query = search_query['query']
    db.search_annotations(query, type)
    pass


@app.route('/case/<case_name>/moclia', methods=['GET'])
def get_moclia_output(case_name):
    return dumps({"message": create_moclia_output(db, case_name), "success": True})


@app.route('/case/<case_name>/selectvariants', methods=['POST'])
def select_variants(case_name):
    variant_object = request.get_json()
    if DEBUG:
        print(dumps(variant_object, indent=2))
    user_id = variant_object.get('userId', None)
    variant_ids = variant_object['selectedSNPVariantIds']
    cnv_ids = variant_object['selectedCNVIds']
    translocation_ids = variant_object['selectedTranslocationIds']
    db.set_not_selected(case_name, user_id)
    if variant_ids is None:
        raise InvalidParameters('Invalid parameter: Variant ids null object', status_code=400)
    for variant_id in variant_ids:
        db.set_selected(variant_id, user_id, 'variants')
    if cnv_ids is None:
        raise InvalidParameters('Invalid parameter: CNV ids null object', status_code=400)

    for cnv_id in cnv_ids:
        db.set_selected(cnv_id, user_id, 'cnvs')
    if translocation_ids is None:
        raise InvalidParameters('Invalid parameter: Translocation ids null object', status_code=400)
    for translocation_id in translocation_ids:
        db.set_selected(translocation_id, user_id, 'translocations')

    return dumps({'message': 'Variant selection successful'})


@app.route('/case/<case_name>/annotation', methods=['GET', 'POST'])
def annotate_case(case_name):
    if request.method == 'GET':
        return dumps(db.get_case_annotation(case_name), indent=2)
    elif request.method == 'POST':
        annotation = request.get_json()
        if DEBUG:
            print(dumps(annotation, indent=2))
        db.set_case_annotation(case_name, annotation['caseAnnotation'])
        return dumps({'message': 'Saved annotation'})


@app.route('/reindex_annotations', methods=['GET'])
def reindex_annotations():
    mongo.set_annotation_flags(db)
    return "Finished reindexing"


@app.route('/variant/<mongo_id>', methods=['GET', 'PUT', 'POST'])
def get_variant_annotations(mongo_id):
    if request.method == 'GET':
        output = db.get_variant_annotations(mongo_id)
        return dumps(output)
    elif request.method == 'POST':
        variant = request.get_json()
        modified_variant = variant
        if DEBUG:
            print(dumps(variant, indent=2))
        if variant['tier'] is not None:
            modified_variant = db.set_tier(mongo_id, variant.get('tier', None), 'variants')
        if variant['notation'] is not None:
            modified_variant = db.set_notation(mongo_id, variant.get('notation'))
        if variant['somaticStatus'] is not None:
            modified_variant = db.set_somatic_status(mongo_id, variant.get('somaticStatus'))
        return dumps(modified_variant)
    elif request.method == 'PUT':
        vcf_annotation = request.get_json()
        print(dumps(vcf_annotation, indent=2))
        return dumps(db.set_canonical_annotation(mongo_id, vcf_annotation))


@app.route('/variant/<mongo_id>/selectannotations', methods=['POST'])
def select_variant_annotations(mongo_id):
    variant = request.get_json()
    if DEBUG:
        print(dumps(variant, indent=2))
    modified_variant = db.select_annotations(variant, 'snp')
    return dumps(modified_variant)


@app.route('/cnv/<mongo_id>', methods=['GET', 'POST'])
def get_cnv_annotations(mongo_id):
    if request.method == 'GET':
        if DEBUG:
            print("Attempting to get cnv annotations")
        output = db.get_cnv_annotations(mongo_id)
        return dumps(output)
    elif request.method == 'POST':
        cnv = request.get_json()
        if DEBUG:
            print(dumps(cnv, indent=2))
        modified_cnv = db.set_tier(mongo_id, cnv.get('tier', None), 'cnvs')
        modified_cnv = db.set_aberration(mongo_id, cnv.get('aberrationType', None))
        return dumps(modified_cnv)


@app.route('/cnv/<mongo_id>/selectannotations', methods=['POST'])
def select_cnv_annotations(mongo_id):
    cnv = request.get_json()
    if DEBUG:
        print(dumps(cnv, indent=2))
    modified_cnv = db.select_annotations(cnv, 'cnv')
    return dumps(modified_cnv)


@app.route('/virus/<mongo_id>', methods=['GET', 'POST'])
def get_virus_annotations(mongo_id):
    if request.method == 'GET':
        output = db.get_virus_annotations(mongo_id)
        return dumps(output)
    elif request.method == 'POST':
        virus = request.get_json()
        # Not doing much here yet
        return dumps(virus)


@app.route('/translocation/<mongo_id>', methods=['GET', 'POST'])
def get_translocation_annotations(mongo_id):
    if request.method == 'GET':
        output = db.get_translocation_annotations(mongo_id)
        return dumps(output)
    elif request.method == 'POST':
        translocation = request.get_json()
        if DEBUG:
            print(dumps(translocation, indent=2))
        modified_translocation = db.set_tier(mongo_id, translocation.get('tier', None), 'translocations')
        modified_translocation = db.set_genes(mongo_id, translocation)
        return dumps(modified_translocation)


@app.route('/translocation/<mongo_id>/selectannotations', methods=['POST'])
def select_translocation_annotations(mongo_id):
    translocation = request.get_json()
    if DEBUG:
        print(dumps(translocation, indent=2))
    modified_translocation = db.select_annotations(translocation, 'translocation')
    return dumps(modified_translocation)


@app.route('/refvariant/<mongo_id>')
def fetch_reference_variant(mongo_id):
    output = db.fetch_reference_variant(mongo_id)
    return dumps(output)


@app.route('/annotation', methods=['POST'])
@app.route('/annotations/', methods=['POST'])
def route_annotations():
    annotation_modified = False
    annot_list = request.get_json()
    new_annotations = []
    modified_annotations = []
    if DEBUG:
        print(dumps(annot_list, indent=2))
    for annotation in annot_list:
        flags = db.save_annotation(annotation)
        if flags[0] is not None:
            mongo.flag_associated_variants(db, annotation)
            annotation_modified = True
            if flags[1] is not None:
                new_annotations.append(flags[1])
            else:
                modified_annotations.append(flags[0])
    # mongo.set_annotation_flags(db)
    print("Finished Saving Annotation")

    return dumps({'message': 'Saved Annotations',
                  'payload': {'annotationModified': annotation_modified,
                              'newAnnotations': new_annotations,
                              'modifiedAnnotations': modified_annotations},
                  'success': True})


@app.route('/annotation/trials', methods=['GET'])
def get_trial_annotations():
    return dumps(db.get_trial_annotations())


@app.route('/annotation/gene/<gene_symbol>', methods=['GET'])
def get_gene_annotations(gene_symbol):
    return dumps(
        {'message': 'Retrieved list of annotations.', 'success': True, 'payload': db.get_gene_annotations(gene_symbol)})


@app.route('/report/<mongo_id>', methods=['GET'])
def fetch_report(mongo_id):
    output = db.fetch_report(mongo_id)
    return dumps(output)


@app.route('/report/<mongo_id>/delete', methods=['GET'])
def delete_report(mongo_id):
    print("Deleting report")
    db.delete_report(mongo_id)
    return "Deleted successfully"


@app.route('/report/<mongo_id>/finalize', methods=['PUT'])
def finalize_report(mongo_id):
    print("Finalizing report")
    finalized_report = db.finalize_report(mongo_id)
    db.update_case_history(finalized_report['caseId'], 4)
    print(dumps(finalized_report))
    return dumps(finalized_report)


@app.route('/report/<mongo_id>/unfinalize', methods=['PUT'])
def unfinalize_report(mongo_id):
    return dumps(db.unfinalize_report(mongo_id))


@app.route('/trials/<nct_id>', methods=['GET'])
def fetch_trial_meta_data(nct_id):
    return dumps(get_trial_metadata(nct_id))


@app.route('/searchVariantNotation/', methods=['POST'])
def search_by_variant_notation():
    search_query = request.get_json()
    oncokb_variant_name = db.search_variant_by_notation(search_query['geneName'], search_query['notation'])
    return dumps({'message': 'Returning oncokbVariantName',
                  'payload': oncokb_variant_name,
                  'success': True})


@app.errorhandler(InvalidParameters)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


if __name__ == "__main__":
    app.run(host='0.0.0.0')
