from pymongo import MongoClient
from variantparser import dbutil as mongo

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = mongo.DbClient(DB_NAME)

ARTIFACT_CATEGORY_NAME = 'Likely Artifact'


def get_annotation_list(variant_type, gene_names, reference_id):
    annotations = {}
    annotation_list = []

    # Handle gene specific annotations
    query = {'isGeneSpecific': True, 'geneId': {'$in': gene_names}, 'caseId': {'$in': [None]},
             'referenceId': {'$in': [None, reference_id]}, 'oncotreeDiagnosis': {'$in': [None]},
             'markedForDeletion': False, 'type': variant_type}
    annotation_list += db.read('annotations', query)
    # print("Gene specific query", query)

    # Get variant annotations
    query = {'isVariantSpecific': True, 'referenceId': reference_id, 'caseId': {'$in': [None]},
             'geneId': {'$in': gene_names + [None]}, 'oncotreeDiagnosis': {'$in': [None]},
             'markedForDeletion': False, 'type': variant_type}
    # print("Variant specific query", query)
    annotation_list += db.read('annotations', query)

    for annotation in annotation_list:
        annotations[str(annotation['_id'])] = annotation
    final_annotations = []
    for annotation in annotations.values():
        final_annotations.append(annotation)
    return final_annotations


def main():
    total_artifacts = 0
    total_variants = 0
    variants = db.read('variants', {})
    for variant in variants:
        total_variants += 1
        utsw_annotations = get_annotation_list('snp', [variant['geneName']],
                                               variant['referenceId'])
        variant['likelyArtifact'] = False
        for annotation in utsw_annotations:
            if annotation['category'] == ARTIFACT_CATEGORY_NAME:
                variant['likelyArtifact'] = True
        if variant['likelyArtifact']:
            total_artifacts += 1
        db. set_artifact_flag(variant['_id'], variant['likelyArtifact'])

    print("Total artifacts found:", total_artifacts, "out of", total_variants, "variants")


if __name__ == '__main__':
    main()
