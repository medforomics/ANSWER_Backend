from pymongo import MongoClient
import string
import uuid
from Bio.SeqUtils import seq3  # Seq3 converts single letter amino acids to 3 letter amino acids
from bson import ObjectId
from bson.json_util import dumps
import pymongo
from answerapi import InvalidParameters
from bson.errors import InvalidId
from .core import ReferenceVariant
from .core import ReferenceTranslocation
from .core import ReferenceCopyNumberVariation
from .core import type_to_collection
from .core import ReferenceVirus
from .copynumbervariation import CopyNumberVariation
from .translocation import Translocation
import maya
import logging


def isAnnotationModified(new_annotation, old_annotation):
    keys = ['text', 'caseId', 'geneId', 'variantId', 'markedForDeletion', 'pmids', 'isTumorSpecific', 'isCaseSpecific',
            "isVariantSpecific", "isGeneSpecific", "category", "classification", "tier", "nctids"]
    for key in keys:
        if new_annotation.get(key, None) != old_annotation.get(key, None):
            return True
    return False


class CaseNotFoundError(Exception):
    def __init__(self, message=None):
        self.message = message


class VariantNotFoundError(Exception):
    def __init__(self, message=None):
        self.message = message


def mda_aa_to_utsw_aa(alteration):
    """
    Converts an MD Anderson alteration to a standard VCF format:
    "Thr227Met" to "T227M"
    :param alteration:
    :return:
    """
    clean_alteration = alteration.replace('*', '')
    if clean_alteration[0] in string.ascii_uppercase:
        clean_alteration = seq3(clean_alteration[0]) + clean_alteration[1:]
    if clean_alteration[-1] in string.ascii_uppercase:
        clean_alteration = clean_alteration[:-1] + seq3(clean_alteration[-1])
    return clean_alteration


def generate_query_filter(query_filter):
    query = {}
    custom_field = None
    if query_filter['field'] == 'geneName':
        for idx in range(len(query_filter['simpleStringValues'])):
            query_filter['simpleStringValues'][idx] = query_filter['simpleStringValues'][idx].upper()
    if query_filter.get('simpleStringValues', False):
        #Guillaume's code here
        # check field like diseaseDatabases or troubledRegions here
        # include if in any disease database, exclude if in any troubled region
        # else do the normal $in
        if query_filter['field'] == 'diseaseDatabases' or query_filter['field'] == 'troubledRegions':
            #$or query
            if query_filter['field'] == 'diseaseDatabases':
                custom_field = '$or'
            else:
                custom_field = '$nor'
            query[custom_field] = list(map(lambda x: {x: {"$eq": True}}, set(query_filter['simpleStringValues'])))
        else:
            #end of G's code
            query['$in'] = query_filter['simpleStringValues']
    if query_filter['minValue'] is not None:
        query['$gte'] = query_filter['minValue'] - 0.00000000005
    if query_filter['maxValue'] is not None:
        query['$lte'] = query_filter['maxValue'] + 0.00000000005

    if query_filter['valueTrue'] and query_filter['valueFalse']:
        pass
    elif query_filter['valueTrue']:
        query['$eq'] = True
    elif query_filter['valueFalse']:
        query['$eq'] = False
    return query, custom_field


class DbClient:
    def __init__(self, db_name='answer'):
        self.client = None
        self.db = self.connect_to_db(db_name)
        self.reference = self.client['reference']

    def connect_to_db(self, db_name):
        """
        Returns a valid db connection
        :return:
        """
        self.client = MongoClient('localhost', 27017)
        db = self.client[db_name]
        return db

    def set_tier(self, mongo_id, tier, collection):
        try:
            _id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        result = self.db[collection].find_one_and_update({'_id': _id}, {'$set': {'tier': tier}},
                                                         return_document=pymongo.ReturnDocument.AFTER)
        return result

    def set_genes(self, mongo_id, translocation):
        try:
            _id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        original_translocation = self.db['translocations'].find_one({'_id': _id})
        left_gene = translocation['leftGene']
        right_gene = translocation['rightGene']
        gene_names = [left_gene, right_gene]
        translocation_name = translocation['fusionName']
        reference_translocation = self.db['reference_translocations'].find_one({'geneNames': gene_names})
        if not reference_translocation:
            new_reference_translocation = {
                'geneNames': gene_names,
                'clinicalCases': [],
                'researchCases': [],
                'annotations': []
            }
            reference_id = self.db['reference_translocations'].insert_one(new_reference_translocation).inserted_id
            reference_translocation = self.db['reference_translocations'].find_one({'_id': reference_id})
        else:
            reference_id = reference_translocation['_id']
        # remove clinical case from current reference translocation
        original_reference_translocation = self.db['reference_translocations'].find_one(
            {'_id': ObjectId(original_translocation['referenceId'])})
        try:
            new_clinical_cases = original_reference_translocation['clinicalCases']
            new_clinical_cases.remove(translocation['caseId'])
        except ValueError:
            new_clinical_cases = original_reference_translocation['clinicalCases']
        update = {'$set': {'clinicalCases': new_clinical_cases}}
        self.db['reference_translocations'].find_one_and_update({'_id': original_reference_translocation['_id']},
                                                                update)
        # Add clinical case to new reference id
        clinical_cases = set(reference_translocation['clinicalCases'])
        clinical_cases.add(translocation['caseId'])
        clinical_cases = list(clinical_cases)
        update = {'$set': {'clinicalCases': clinical_cases}}
        self.db['reference_translocations'].find_one_and_update({'_id': reference_id}, update)
        # Set fields on translocation
        num_cases_seen = len(clinical_cases)
        update = {'$set': {'numCasesSeen': num_cases_seen,
                           'leftGene': left_gene,
                           'rightGene': right_gene,
                           'referenceId': reference_id,
                           'fusionName': translocation_name
                           }
                  }
        return self.db['translocations'].find_one_and_update({'_id': _id}, update,
                                                             return_document=pymongo.ReturnDocument.AFTER)


    def set_somatic_status(self, mongo_id, somatic_status):
        try:
            _id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        result = self.db['variants'].find_one_and_update({'_id': _id}, {'$set': {'somaticStatus': somatic_status}})

        return result


    def set_notation(self, mongo_id, notation):
        try:
            _id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        result = self.db['variants'].find_one_and_update({'_id': _id}, {'$set': {'notation': notation}})

        return result

    def update_fpkm_diagnosis(self, case_name, diagnosis):
        query = {'caseId': case_name}
        update = {'$set': {'oncotreeDiagnosis': diagnosis}}
        self.db['fpkm'].update_many(query, update)
        pass

    def update_case_details(self, case_update, case_name):
        query = {'caseId': case_name}
        projection = {'annotation': 0, 'clinicalTrials': 0}
        case = self.read('cases', query, projection)[0]
        diagnosis = case_update.get('oncotreeDiagnosis', case.get('oncotreeDiagnosis', None))
        self.update_fpkm_diagnosis(case_name, diagnosis)
        dedup_average_depth = case_update.get('dedupAvgDepth', case.get('dedupAvgDepth', None))
        dedup_pct_over_100x = case_update.get('dedupPctOver100X', case.get('dedupPctOver100X', None))
        tumor_tissue_type = case_update.get('tumorTissueType', case.get('tumorTissueType', None))
        tumor_percent = case_update.get('tumorPercent', case.get('tumorPercent', '100'))
        tmb_class = case_update.get('tmbClass', case.get('tmbClass', None))
        msi_class = case_update.get('msiClass', case.get('msiClass', None))
        active = case_update.get('active', case.get('active', True))
        icd10 = case_update.get('icd10', case.get('icd10', None))

        update = {'$set': {'oncotreeDiagnosis': diagnosis, 'dedupAvgDepth': dedup_average_depth,
                           'dedupPctOver100X': dedup_pct_over_100x, 'active': active,
                           'tumorTissueType': tumor_tissue_type, 'tumorPercent': tumor_percent,
                           'icd10': icd10,
                           'tmbClass': tmb_class,
                           'msiClass': msi_class}}
        return self.db.cases.find_one_and_update({'caseId': case_update['caseId']}, update,
                                                 return_document=pymongo.ReturnDocument.AFTER)

    def update_diagnosis(self, case):  # Deprecated
        diagnosis = case.get('oncotreeDiagnosis', None)
        update = {'$set': {'oncotreeDiagnosis': diagnosis}}
        return self.db.cases.find_one_and_update({'caseId': case['caseId']}, update,
                                                 return_document=pymongo.ReturnDocument.AFTER)

    def get_reports(self, case_id):
        return self.read('reports', {'caseId': case_id},
                         {'_id': 1, 'createdBy': 1, 'modifiedBy': 1, 'dateCreated': 1,
                          'dateModified': 1, 'reportName': 1, 'summary': 1, 'finalized': 1, 'amended': 1,
                          'addendum': 1, 'dateFinalized': 1, 'amendmentReason': 1})  # Add Details

    def fetch_report(self, mongo_id):
        report_id = ObjectId(mongo_id)
        return self.db.reports.find_one(report_id)

    def delete_report(self, mongo_id):
        report_id = ObjectId(mongo_id)
        self.db.reports.delete_one({'_id': report_id})
        pass

    def create_report(self, case_name, report):
        print("Creating Report")
        print(report)
        now = maya.now().iso8601()
        report['dateCreated'] = now
        report['dateModified'] = now
        report.pop('_id')
        inserted_id = self.db.reports.insert_one(report).inserted_id
        inserted_report = self.db.reports.find_one({'_id': inserted_id})
        return inserted_report

    def update_report(self, case_name, report):
        print("Updating Report")
        print(report)
        report_id = ObjectId(report['_id'])
        report.pop('_id')
        now = maya.now().iso8601()
        report['dateModified'] = now
        self.db.reports.replace_one({'_id': report_id}, report)
        modified_report = self.db.reports.find_one(report_id)
        return modified_report

    def finalize_report(self, mongo_id):
        report = self.fetch_report(mongo_id)
        finalized_reports = self.read('reports', {'caseId': report['caseId'], 'finalized': True,
                                                  'amended': {'$in': [None, False]}})
        if len(finalized_reports) != 0 and not report['addendum']:
            print(mongo_id, "report failed to finalize")
            return {'error': 'report already finalized'}

        return self.db.reports.find_one_and_update({'_id': report['_id']},
                                                   {'$set': {'finalized': True, 'dateFinalized': maya.now().iso8601()}},
                                                   return_document=pymongo.ReturnDocument.AFTER)

    def unfinalize_report(self, mongo_id):
        report = self.fetch_report(mongo_id)
        return self.db.reports.find_one_and_update({'_id': report['_id']}, {'$set': {'finalized': False}},
                                                   return_document=pymongo.ReturnDocument.AFTER)

    def is_mda_gene(self, gene):
        mda_gene = self.client.reference.mda.find_one({'symbol': gene})
        if mda_gene is not None:
            return True
        else:
            return False

    def get_tissue_oncotree_codes(self, oncotree_diagnosis):
        reference_oncotree = self.client.reference.oncotree.find_one({'code': oncotree_diagnosis})
        return reference_oncotree['tissueCodes']

    def get_mda_transcript(self, gene_name):
        print(gene_name)

        mda_symbol = self.client.reference.mda.find_one({'symbol': gene_name})['geneId']
        ensembl_entry = self.client.reference.ensembl.find_one({'rnaNucleotideAccession.identifier': mda_symbol})
        return ensembl_entry['ensemblRnaIdentifier']['identifier']

    def save_annotation(self, annotation):
        print("Saving Annotation:")
        print(annotation)
        annotation_modified = None
        annotation_created = None
        _id = annotation.pop('_id', None)
        variant_id = annotation.get('variantId', None)
        collection_name = type_to_collection(annotation.get('type', None))
        if collection_name is None:
            print("Collection not found")
        if variant_id is not None:
            query = {'_id': ObjectId(variant_id)}
            variant = self.db[collection_name].find_one(query)
            annotation['referenceId'] = variant['referenceId']
        else:
            annotation['referenceId'] = None
        if _id is None:
            now = maya.now().iso8601()
            annotation['createdDate'] = now
            annotation['modifiedDate'] = now
            annotation_created = self.db['annotations'].insert_one(annotation).inserted_id
            annotation_modified = annotation_created
        else:
            _id = _id['$oid']
            query = {'_id': ObjectId(_id)}
            db_annotation = self.db['annotations'].find_one(query)
            if isAnnotationModified(annotation, db_annotation):
                annotation_modified = ObjectId(_id)
                annotation['modifiedDate'] = maya.now().iso8601()

            self.db['annotations'].replace_one({'_id': ObjectId(_id)}, annotation)
        return (annotation_modified, annotation_created)

    def get_case(self, case_id):
        query = {'caseId': case_id}
        case = self.db['cases'].find_one(query)
        return case

    def insert(self, collection, payload):
        collection = self.db[collection]
        insert_id = collection.insert_one(payload)
        return insert_id

    def insert_many(self, collection, payload):
        collection = self.db[collection]
        insert_ids = collection.insert_many(payload)
        return insert_ids

    def read(self, collection, query, db_filter=None):
        collection = self.db[collection]
        cursor = collection.find(query, db_filter)
        result_list = []
        for result in cursor:
            result_list.append(result)
        return result_list

    def count_cases(self):
        cursor = self.db.cases.find()
        return cursor.count()

    def fetch_appris_data(self, transcript):
        query = {'transcript_identifier': transcript}
        result = self.client.reference.appris.find(query)
        if result.count() == 0:
            item = {}
        else:
            item = result.next()
        return item.get('reliability_labels', 'None'), item.get('tsl', '-')

    def update_assigned_users(self, case_id, user_ids):
        collection = self.db.cases
        case = collection.find_one({'caseId': case_id})
        if not case.get('caseHistory', None):
            self.update_case_history(case_id, 1)
        result = collection.find_one_and_update({'caseId': case_id}, {'$set': {'assignedTo': user_ids}},
                                                projection={'_id': 0, 'annotation': 0},
                                                return_document=pymongo.ReturnDocument.AFTER)
        return result

    def update_assigned_groups(self, case_id, group_ids):
        collection = self.db.cases
        result = collection.find_one_and_update({'caseId': case_id}, {'$set': {'groupIds': group_ids}},
                                                projection={'_id': 0, 'annotation': 0},
                                                return_document=pymongo.ReturnDocument.AFTER)
        return result

    def get_reference_cnv(self, cnv):
        collection = self.db.reference_cnv
        query = {'geneNames': cnv.genes, 'aberrationType': cnv.aberration_type}
        cursor = collection.find(query)
        if cursor.count() == 0:
            return None
        else:
            reference_cnv = ReferenceCopyNumberVariation(cursor.next())
            return reference_cnv
        pass

    def get_reference_virus(self, virus):
        collection = self.db.reference_viruses
        query = {'virusName': virus.virus_name}
        cursor = collection.find(query)
        if cursor.count() == 0:
            return None
        else:
            reference_virus = ReferenceVirus(cursor.next())
            return reference_virus

    def get_reference_translocation(self, translocation):
        collection = self.db.reference_translocations
        query = {'geneNames': {'$all': translocation.gene_names}}
        cursor = collection.find(query)
        if cursor.count() == 0:
            return None
        else:
            reference_translocation = ReferenceTranslocation(cursor.next())
            return reference_translocation

    def get_reference_variant(self, variant):
        collection = self.db.reference_variants
        query = {'chrom': variant.chrom, 'pos': variant.pos, 'alt': variant.alt, 'type': variant.type,
                 'reference': variant.reference}
        cursor = collection.find(query)
        if cursor.count() == 0:
            return None
        elif cursor.count() > 1:
            raise Exception
        else:
            reference_variant = ReferenceVariant(cursor.next())
            if variant.reference != reference_variant.reference:
                logger = logging.getLogger("AnswerReceiver")
                logger.warning("Found ambiguous reference in case")
                collection = self.db.annotations
                query = {'referenceId': ObjectId(reference_variant.mongo_id)}
                cursor = collection.find(query)
                logger.warning(str(cursor.count()) + " Annotations could be affected.")
                for annotation in cursor:
                    logger.info(annotation['text'])
                # logger.info(str(variant.as_mongo_dict()))
                logger.info(str(reference_variant.as_mongo_dict()))
            return reference_variant

    def create_reference_virus(self, reference_virus):
        payload = reference_virus.as_mongo_dict()
        reference_id = self.insert('reference_viruses', payload).inserted_id
        return reference_id

    def create_reference_translocation(self, reference_translocation):
        payload = reference_translocation.as_mongo_dict()
        reference_id = self.insert('reference_translocations', payload).inserted_id
        return reference_id

    def create_reference_variant(self, reference_variant):
        payload = reference_variant.as_mongo_dict()
        reference_id = self.insert('reference_variants', payload).inserted_id
        return reference_id

    def create_reference_cnv(self, reference_cnv):
        payload = reference_cnv.as_mongo_dict()
        reference_id = self.insert('reference_cnv', payload).inserted_id
        return reference_id

    def create_cnv(self, case_id, new_cnv):
        # create CNV from gene name
        gene_names = new_cnv['genes']
        print("Creating CNV from gene names")
        gencode_genes = []
        starts = []
        ends = []
        chrom = None
        for gene_name in gene_names:
            query = {'geneNameUpper': gene_name.upper()}
            gencode_gene = self.reference.gencode.find_one(query)
            if gencode_gene is None:
                return {"message": "Unable to find gene " + gene_name, "success": False}
            gencode_genes.append(gencode_gene['geneName'])
            starts.append(gencode_gene['start'])
            ends.append(gencode_gene['end'])

            if chrom is not None:
                if chrom != gencode_gene['chrom']:
                    return {"message": "Not all genes are on the same chromosome", "success": False}
            chrom = gencode_gene['chrom']

        start = min(starts)
        end = max(ends)
        cytoband = self.get_cytoband(chrom, start, end)
        cnv_dict = {
            'genes': gencode_genes,
            'chrom': chrom,
            'start': start,
            'end': end,
            'copyNumber': new_cnv['copyNumber'],
            'aberrationType': new_cnv['aberrationType'],
            'caseId': case_id,
            'referenceId': None,
            'score': None,
            'cytoband': cytoband,
        }
        cnv = CopyNumberVariation(gencode_genes, None, case_id, None, cnv_dict)
        reference_cnv = self.get_reference_cnv(cnv)
        if reference_cnv is None:
            reference_cnv = ReferenceCopyNumberVariation.from_copy_number_variation(cnv)
            reference_cnv.mongo_id = self.create_reference_cnv(reference_cnv)
        reference_cnv.assign_case(case_id, 'Clinical', self.db)
        cnv.reference_id = reference_cnv.mongo_id
        cnv.num_cases_seen = len(reference_cnv.clinical_cases)
        self.db.cnvs.insert_one(cnv.as_mongo_dict())
        return {"message": "CNV successfully created", "success": True}
    
    ### added by G ###
    def create_ftl(self, case_id, new_ftl):
        print("Creating FTL")
        ftl_dict = {
            'caseId': new_ftl['caseId'],
            'leftGene': new_ftl['leftGene'],
            'rightGene': new_ftl['rightGene'],
            'fusionName': new_ftl['fusionName'],
            'leftExons': new_ftl['leftExons'],
            'rightExons': new_ftl['rightExons'],
        }
        ftl = Translocation(None, case_id, None, ftl_dict)
        reference_ftl = self.get_reference_translocation(ftl)
        if reference_ftl is None:
            reference_ftl = ReferenceTranslocation.from_translocation(ftl)
            reference_ftl.mongo_id = self.create_reference_translocation(reference_ftl)
        reference_ftl.assign_case(case_id, 'Clinical', self.db)
        ftl.reference_id = reference_ftl.mongo_id
        ftl.num_cases_seen = len(reference_ftl.clinical_cases)
        self.db.translocations.insert_one(ftl.as_mongo_dict())
        return {"message": "FTL successfully create", "success": True}
    #########

    def insert_case(self, case):
        self.insert('cases', case.as_mongo_dict())
        variants_for_insert = []
        for variant in case.variants:
            variants_for_insert.append(variant.as_mongo_dict())
        if variants_for_insert:
            self.insert_many('variants', variants_for_insert)
        translocations_for_insert = []
        for translocation in case.translocations:
            translocations_for_insert.append(translocation.as_mongo_dict())
        if translocations_for_insert:
            self.insert_many('translocations', translocations_for_insert)
        fpkm_for_insert = []
        for fpkm in case.fpkm:
            fpkm_for_insert.append(fpkm.as_mongo_dict())
        if fpkm_for_insert:
            self.insert_many('fpkm', fpkm_for_insert)
        ballele_freq_for_insert = []
        for ballele_freq in case.ballele_freq:
            ballele_freq_for_insert.append(ballele_freq.as_mongo_dict())
        if ballele_freq_for_insert:
            self.insert_many('ballelefreqs', ballele_freq_for_insert)
        cnvs_for_insert = []
        for cnv in case.copy_number_variations:
            cnvs_for_insert.append(cnv.as_mongo_dict())
        if cnvs_for_insert:
            self.insert_many('cnvs', cnvs_for_insert)
        self.insert('cnv_plot', {'caseId': case.case_id, 'cnr': case.cnr, 'cns': case.cns})
        viruses_for_insert = []
        for virus in case.viruses:
            viruses_for_insert.append(virus.as_mongo_dict())
        if viruses_for_insert:
            self.insert_many('viruses', viruses_for_insert)
        mutational_signatures_for_import = []
        for mutational_signature in case.mutational_signatures:
            mutational_signatures_for_import.append(mutational_signature.as_mongo_dict())
        if mutational_signatures_for_import:
            self.insert_many('mutational_signatures', mutational_signatures_for_import)

    def annotate_case(self, case):
        case_id = case.case_id
        cursor = self.db.cnvs.find({'caseId': case_id})
        self.db.variants.update_many({'caseId': case_id}, {'$set': {'relatedCNV': None}})
        for cnv in cursor:
            if cnv['aberrationType'].lower() == 'itd':
                pass
            else:
                output_cnv = {'_id': cnv['_id'], 'copyNumber': cnv['copyNumber'],
                              'aberrationType': cnv['aberrationType']}
                self.db.variants.update_many(
                    {'caseId': case_id, 'chrom': cnv['chrom'], 'pos': {'$gte': cnv['start'], '$lte': cnv['end']}}, {
                        '$set': {'relatedCNV': output_cnv, 'copyNumber': cnv['copyNumber']}})
        pass

    def set_aberration(self, mongo_id, aberration_type):
        try:
            _id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        collection = self.db.cnvs
        result = collection.find_one_and_update({'_id': _id}, {'$set': {'aberrationType': aberration_type}},
                                                return_document=pymongo.ReturnDocument.AFTER)
        return result

    def get_case_by_name(self, case_name):
        query = {"caseName": case_name}
        cursor = self.db.cases.find(query)
        if cursor.count() == 0:
            return None
        else:
            result_list = []
            for result in cursor:
                result_list.append(result)
            return result_list[0]

    def update_variant(self, variant):
        if variant.get('score', None) is None:
            collection = 'cnvs'
        else:
            collection = 'variants'
        self.db[collection].find_one_and_update(
            {'_id': variant['_id']},
            {'$set': {'mdaAnnotation': variant['mdaAnnotation'], 'mdaAnnotated': variant['mdaAnnotated']}}
        )
        pass

    def get_gene_annotations(self, gene_symbol):
        query = {'isGeneSpecific': True, 'geneId': {'$in': [gene_symbol]},
                 'markedForDeletion': False}
        annotation_list = self.read('annotations', query)
        print("Gene specific query", query)
        return annotation_list

    def fetch_reference_variant(self, mongo_id):
        try:
            _id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        query = {'_id': _id}
        ref_variant = self.db['reference_variants'].find_one(query)
        annotations = self.read('annotations', {'referenceId': _id, 'markedForDeletion': False})
        ref_variant['utswAnnotations'] = annotations
        return ref_variant

    def check_if_annotated(self, variant):
        gene_name = variant['geneName']
        case_id = variant['caseId']
        reference_id = variant['referenceId']
        query = {'isGeneSpecific': True, 'geneId': gene_name, 'caseId': {'$in': [None, case_id]},
                 'referenceId': {'$in': [None, reference_id]}, 'markedForDeletion': False}
        if self.db['annotations'].find_one(query):
            return True
        query = {'isVariantSpecific': True, 'referenceId': reference_id, 'caseId': {'$in': [None, case_id]},
                 'geneId': {'$in': [None, gene_name]}, 'markedForDeletion': False}
        if self.db['annotations'].find_one(query):
            return True
        return False

    def search_annotations(self, search, type):
        if type == 'snp':
            chrom = search['chrom']
            pos = search['pos']
            alt = search['alt']
            reference_variant = self.db['reference_variants'].find_one({'chrom': chrom, 'pos': pos, 'alt': alt})
            return self.get_annotation_list(type, None, search['genes'], reference_variant['_id'])
        pass

    def get_annotation_list(self, variant_type, case_id, gene_names, reference_id, oncotree):

        annotations = {}
        annotation_list = []
        # Handle case specific annotations
        query = {'isCaseSpecific': True, 'caseId': case_id, 'geneId': {'$in': gene_names + [None]},
                 'referenceId': {'$in': [None, reference_id]}, 'oncotreeDiagnosis': {'$in': [None, oncotree]},
                 'markedForDeletion': False, 'type': variant_type}
        # print("Case Specific Query:", query)
        annotation_list += self.read('annotations', query)

        # Handle gene specific annotations
        query = {'isGeneSpecific': True, 'geneId': {'$in': gene_names}, 'caseId': {'$in': [None, case_id]},
                 'referenceId': {'$in': [None, reference_id]}, 'oncotreeDiagnosis': {'$in': [None, oncotree]},
                 'markedForDeletion': False, 'type': variant_type}
        annotation_list += self.read('annotations', query)
        # print("Gene specific query", query)

        # Get variant annotations
        query = {'isVariantSpecific': True, 'referenceId': reference_id, 'caseId': {'$in': [None, case_id]},
                 'geneId': {'$in': gene_names + [None]}, 'oncotreeDiagnosis': {'$in': [None, oncotree]},
                 'markedForDeletion': False, 'type': variant_type}
        # print("Variant specific query", query)
        annotation_list += self.read('annotations', query)

        for annotation in annotation_list:
            annotations[str(annotation['_id'])] = annotation
        final_annotations = []
        for annotation in annotations.values():
            final_annotations.append(annotation)
        return final_annotations

    def get_cnv_annotations(self, mongo_id):
        try:
            cnv_id = ObjectId(mongo_id)
        except InvalidId:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        query = {'_id': cnv_id}
        logger = logging.getLogger("AnswerReceiver")
        logger.debug("Retrieving CNV")
        result = self.read('cnvs', query)
        if len(result) != 1:
            raise InvalidParameters('Unable to locate CNV', status_code=400)
        cnv = result[0]
        output_dict = cnv
        reference_id = cnv.get('referenceId', None)
        gene_names = cnv['genes']
        case_id = cnv['caseId']
        case = self.get_case(case_id)
        oncotree = case['oncotreeDiagnosis']
        if reference_id is not None:
            logger.debug("Retrieving reference CNV for id: ", reference_id)
            reference_cnv = self.db['reference_cnv'].find_one({'_id': reference_id})
        output_dict['referenceCnv'] = reference_cnv
        logger.debug("Retrieving Annotations")
        output_dict['referenceCnv']['utswAnnotations'] = self.get_annotation_list('cnv', case_id, gene_names,
                                                                                  reference_id, oncotree)
        return output_dict

    def get_virus_annotations(self, mongo_id):
        try:
            virus_id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        query = {'_id': virus_id}
        result = self.read('viruses', query)
        if len(result) != 1:
            raise InvalidParameters('Unable to locate virus.', status_code=400)
        virus = result[0]
        output_dict = virus
        reference_id = virus.get('referenceId', None)
        virus_name = virus['VirusName']
        case_id = virus['caseId']
        case = self.get_case(case_id)
        oncotree = case['oncotreeDiagnosis']
        if reference_id is not None:
            reference_virus = self.db['reference_viruses'].find_one({'_id': reference_id})
        output_dict['referenceVirus'] = reference_virus
        output_dict['referenceVirus']['utswAnnotations'] = self.get_annotation_list('virus',
                                                                                    case_id,
                                                                                    [virus_name],
                                                                                    reference_id,
                                                                                    oncotree)
        return output_dict

    def get_translocation_annotations(self, mongo_id):
        try:
            translocation_id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        query = {'_id': translocation_id}
        result = self.read('translocations', query)
        if len(result) != 1:
            raise InvalidParameters('Unable to locate translocation', status_code=400)
        translocation = result[0]
        output_dict = translocation
        reference_id = translocation.get('referenceId', None)
        gene_names = [translocation['leftGene'], translocation['rightGene']]
        case_id = translocation['caseId']
        case = self.get_case(case_id)
        oncotree = case['oncotreeDiagnosis']
        if reference_id is not None:
            reference_translocation = self.db['reference_translocations'].find_one({'_id': reference_id})
        output_dict['referenceTranslocation'] = reference_translocation
        output_dict['referenceTranslocation']['utswAnnotations'] = self.get_annotation_list('translocation',
                                                                                            case_id,
                                                                                            gene_names,
                                                                                            reference_id, oncotree)

        return output_dict

    def get_variant_annotations(self, mongo_id):
        try:
            variant_id = ObjectId(mongo_id)
        except InvalidId as error:
            raise InvalidParameters('Invalid Mongo ID', status_code=400)
        query = {'_id': variant_id}
        projection = {'infoFields': 0}
        result = self.read('variants', query, projection)
        if len(result) != 1:
            raise InvalidParameters('Unable to locate variant', status_code=400)
        variant = result[0]
        output_dict = variant
        reference_id = variant.get('referenceId', None)
        gene_name = variant['geneName']
        case_id = variant['caseId']
        case = self.get_case(case_id)
        oncotree = case['oncotreeDiagnosis']
        reference_variant = self.db['reference_variants'].find_one({'_id': reference_id})
        output_dict['referenceVariant'] = reference_variant
        output_dict['referenceVariant']['utswAnnotations'] = self.get_annotation_list('snp', case_id, [gene_name],
                                                                                      reference_id,
                                                                                      oncotree)

        return output_dict

    def change_contact_to_utsw(self, trial):
        utsw_trial = self.reference.studyfinder.find_one({'nctId': trial['nctid']})
        trial['dept'] = "UTSW Cancer Center"
        trial['pi'] = utsw_trial['contactOverride'] + ' or ' + utsw_trial['contactOverrideLastName']

    def trial_at_utsw(self, trial):
        # Check reference DB for NCT_ID and then swap contact information
        cursor = self.reference.studyfinder.find({'nctId': trial['nctid']})
        return cursor.count() != 0

    def format_trials(self, mda_report):
        trial_keys = ['relevantBiomarkers', 'selectedBiomarkers', 'selectedAdditionalBiomarkers',
                      'relevantAdditionalBiomarkers']
        for key in trial_keys:
            if mda_report[key] is not None:
                for trial in mda_report[key]:
                    if self.trial_at_utsw(trial):
                        self.change_contact_to_utsw(trial)
                    else:
                        trial['dept'] = 'MD Anderson Department of ' + trial['dept']

    def add_trials(self, case_id, mda_report):
        self.format_trials(mda_report)
        self.db.cases.find_one_and_update({'caseId': case_id},
                                          {'$set': {
                                              'clinicalTrials': {'relevantBiomarkers': mda_report['relevantBiomarkers'],
                                                                 'selectedBiomarkers': mda_report['selectedBiomarkers'],
                                                                 'selectedAdditionalBiomarkers': mda_report[
                                                                     'selectedAdditionalBiomarkers'],
                                                                 'relevantAdditionalBiomarkers': mda_report[
                                                                     'relevantAdditionalBiomarkers']}}})
        pass

    def get_case_version(self, case_name):
        return 2

    def create_utsw_uuid(self, case_id):
        query = {'caseId': case_id}
        utsw_id = str(uuid.uuid4())
        update = {'$set': {'utswUuid': utsw_id}}
        self.db.cases.find_one_and_update(query, update)
        pass

    def find_mda_case(self, mda_report):
        utsw_id = mda_report['mrn'].strip().split('_')[1].strip().replace('\\r', '').replace('\\n', '').strip()
        print('Looking for utsw ID,', utsw_id)
        query = {"utswUuid": utsw_id}
        result = self.read('cases', query)
        if len(result) != 1:
            print(len(result))
            for item in result:
                print(result['caseId'])
            raise CaseNotFoundError
        return result[0]['caseId']

    def find_mda_cnv(self, row_dict, case_name):
        gene_name = row_dict['gene']
        abberation = row_dict['alteration']
        print("Looking for CNV in gene", gene_name)
        print("With abberation", abberation)
        if abberation == 'Deletion':
            query = {'caseId': case_name, 'copyNumber': {'$lt': 2}, 'genes': gene_name, 'selected': True}
            print(query)
            cnv = self.db.cnvs.find_one(query)
        else:
            query = {'caseId': case_name, 'copyNumber': {'$gt': 2}, 'genes': gene_name, 'selected': True}
            cnv = self.db.cnvs.find_one(query)
        if cnv is None:
            raise VariantNotFoundError("Searching for CNV but no matching CNV found")
        return cnv

    def find_mda_variant(self, row_dict, case_name):
        """
        Makes multiple attempts to find a variant to assign an MDA anderson annotation to
        :param row_dict:
        :param case_name:
        :return:
        """
        if (row_dict['alteration'].startswith('c.')):
            clean_alteration = row_dict['alteration']
        elif row_dict['alteration'] == 'Deletion':
            return self.find_mda_cnv(row_dict, case_name)
        else:
            clean_alteration = mda_aa_to_utsw_aa(row_dict['alteration'])
        gene_name = row_dict['gene']
        print("Looking for variant in gene", gene_name)
        print("With alteration", clean_alteration)
        query = {"geneName": gene_name, "caseId": case_name}
        cursor = self.db.variants.find(query)
        for variant in cursor:
            # print(variant)
            for annotation in variant['vcfAnnotations']:
                for key, value in annotation.items():
                    if clean_alteration in value:
                        return variant

        cursor = self.db.variants.find({'caseId': case_name})
        for variant in cursor:
            variant_alt = variant['infoFields'].get('AA', '')
            if row_dict['alteration'] in variant_alt:
                return variant
        raise VariantNotFoundError('Searching for gene ' + gene_name + ' in case ' + case_name +
                                   ' but no matching variant was found')

    def set_not_selected(self, case_name: str, user_id):
        query = {"caseId": case_name}
        time_stamp = maya.now().iso8601()
        if user_id is None:
            update = {'$set': {'selected': False}}
        else:
            update_dict = {}
            update_dict['annotatorSelections.' + user_id] = False
            update_dict['annotatorDates.' + user_id] = time_stamp

            update = {
                '$set': update_dict
            }
            self.db.variants.update_many(query, update)
            self.db.translocations.update_many(query, update)
            self.db.cnvs.update_many(query, update)

        pass

    def set_selected(self, mongo_id: str, user_id, collection):
        time_stamp = maya.now().iso8601()
        query = {'_id': ObjectId(mongo_id)}
        if user_id is None:
            update = {'$set': {'selected': True}}
        else:
            update_dict = {}
            update_dict['annotatorSelections.' + user_id] = True
            update_dict['annotatorDates.' + user_id] = time_stamp
            update = {
                '$set': update_dict
            }

        self.db[collection].find_one_and_update(query, update)

    def get_all_selected(self, case):

        case_id = case['caseId']
        variantProjection = {'chrom': 1, 'pos': 1, 'reference': 1, 'alt': 1, 'tumorTotalDepth': 1,
                             'tumorAltFrequency': 1, 'geneName': 1, 'notation': 1}
        JEFF_GAGAN_PROD_USER_ID = '5'
        case_owner = case.get('caseOwner', JEFF_GAGAN_PROD_USER_ID)
        query = {'annotatorSelections.' + case_owner: True, 'caseId': case_id}
        variants = self.read('variants', query, variantProjection)
        cnvs = self.read('cnvs', query)
        translocations = self.read('translocations', query)
        return {'variants': variants, 'cnvs': cnvs, 'translocations': translocations}

    def get_selected(self, case_id, type):
        query = {'caseId': case_id, 'selected': True}
        return self.read(type, query)

    def select_annotations(self, variant, type):
        collection_name = type_to_collection(type)
        collection = self.db[collection_name]
        new_list = []
        for object_id in variant['annotationIdsForReporting']:
            new_list.append(ObjectId(object_id['$oid']))
        variant['annotationIdsForReporting'] = new_list
        return collection.find_one_and_update({'_id': ObjectId(variant['_id']['$oid'])},
                                              {'$set': {
                                                  'annotationIdsForReporting': variant[
                                                      'annotationIdsForReporting']}},
                                              return_document=pymongo.ReturnDocument.AFTER)

    def update_case_history(self, case_name, new_status):
        time_stamp = maya.now().iso8601()
        case = self.db.cases.find_one({'caseId': case_name})
        history = case.get('caseHistory', None)
        if history is not None:
            if history[-1]['step'] == new_status:
                return case
        update = {'$push': {'caseHistory': {'step': new_status, 'time': time_stamp}}}
        query = {'caseId': case_name}
        return self.db.cases.find_one_and_update(query, update)

    def set_case_owner(self, case_name, user_id):
        query = {'caseId': case_name}
        update = {'$set': {'caseOwner': user_id}}
        return self.db['cases'].find_one_and_update(query, update, return_document=pymongo.ReturnDocument.AFTER)

    def get_case_annotation(self, case_name: str):
        query = {"caseId": case_name}
        case_details = self.db.cases.find_one(query)
        if not case_details:
            raise InvalidParameters('Case not found', status_code=400)
        annotation = case_details.get('annotation', {'caseAnnotation': None})
        annotation['assignedTo'] = case_details['assignedTo']
        annotation['caseId'] = case_details['caseId']
        return annotation

    def set_case_annotation(self, case_name, annotation):
        query = {'caseId': case_name}
        case_details = self.db.cases.find_one(query)
        if not case_details:
            raise InvalidParameters('Case not found', status_code=400)
        existing_annotation = case_details.get('annotation', None)
        update = {
            '$set': {'annotation.caseAnnotation': annotation, 'annotation.dateModified': maya.now().iso8601()}}
        if existing_annotation is None:
            update['$set']['annotation.dateCreated'] = maya.now().iso8601()
        self.db.cases.find_one_and_update(query, update)
        pass

    def set_artifact_flag(self, variant_id, artifact_flag):
        query = {'_id': ObjectId(variant_id)}
        update = {
            '$set': {'likelyArtifact': artifact_flag}
        }
        self.db.variants.find_one_and_update(query, update)
        pass

    def get_trial_annotations(self):
        query = {'category': 'Clinical Trial'}
        result = {'success': True,
                  'payload': self.read('annotations', query),
                  "message": "Clinical Trial Annotations",
                  }
        return result

    def create_indices(self):
        index = [("chrom", pymongo.ASCENDING), ("pos", pymongo.ASCENDING), ("alt", pymongo.ASCENDING)]
        self.db.reference_variants.create_index(index)
        index = [("geneName", pymongo.ASCENDING)]
        self.db.variants.create_index(index)
        pass

    def get_cytoband(self, chrom, start, end):
        query = {'chrom': chrom, 'start': {'$lte': start}, 'end': {'$gte': start}}
        start_band = self.reference.cytoband.find_one(query)
        query = {'chrom': chrom, 'start': {'$lte': end}, 'end': {'$gte': end}}
        end_band = self.reference.cytoband.find_one(query)
        cytoband_string = start_band['cytoBand'] + '-' + end_band['cytoBand']
        return cytoband_string

    def set_canonical_annotation(self, mongo_id, vcf_annotation):
        variant = self.db.variants.find_one({'_id': ObjectId(mongo_id)})
        if variant is None:
            return {'message': "Unable to find variant", 'success': False}
        new_canonical = 0
        for idx, annotation in enumerate(variant['vcfAnnotations']):
            if annotation['featureId'] == vcf_annotation['featureId']:
                new_canonical = idx
        tmp = variant['vcfAnnotations'][0]
        variant['vcfAnnotations'][0] = variant['vcfAnnotations'][new_canonical]
        variant['vcfAnnotations'][new_canonical] = tmp
        variant['effects'] = variant['vcfAnnotations'][0]['effects']
        variant['impact'] = variant['vcfAnnotations'][0]['impact']
        variant['geneName'] = variant['vcfAnnotations'][0]['geneName']
        variant['notation'] = variant['vcfAnnotations'][0]['proteinNotation']
        variant['rank'] = variant['vcfAnnotations'][0]['rank']
        if variant['notation'] == '':
            variant['notation'] = variant['vcfAnnotations'][0]['codingNotation']
        self.db.variants.replace_one({'_id': ObjectId(mongo_id)}, variant)
        reference_id = variant['referenceId']
        self.db.reference_variants.find_one_and_update({'_id': reference_id},
                                                       {'$set': {'preferredTranscript': variant[
                                                           'vcfAnnotations'][0]['featureId']}})
        return ({"message": "Canonical vcf annotation changed", "success": True})

    def search_variant_by_notation(self, gene, notation):
        oncokb_names = self.read('variants',
                                 {'geneName': gene, 'notation': notation, 'oncokbVariantName': {'$ne': None}})
        if len(oncokb_names) > 0:
            return oncokb_names[0]['oncokbVariantName']
        return None


def flag_associated_variants(client: DbClient, annotation: {}):
    query = {}
    if annotation['isCaseSpecific']:
        query['caseId'] = annotation['caseId']
    if annotation['isGeneSpecific']:
        query['geneName'] = annotation['geneId']
    if annotation['isVariantSpecific']:
        query['referenceId'] = annotation['referenceId']
    if query:
        collection_name = type_to_collection(annotation.get('type', 'snp'))
        client.db[collection_name].update_many(query, {'$set': {'utswAnnotated': True}})


def set_annotation_flags(client: DbClient):
    """
    Resets all the flags for variants in the DB based on existing annotations
    :param db:
    :return:
    """
    query = {'markedForDeletion': False}
    annotations = client.read('annotations', query)
    update = {'$set': {'utswAnnotated': False}}
    client.db.variants.update_many({}, update)
    client.db.translocations.update_many({}, update)
    client.db.cnvs.update_many({}, update)
    for annotation in annotations:
        flag_associated_variants(client, annotation)
    pass
