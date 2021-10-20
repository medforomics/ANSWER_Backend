from pymongo import MongoClient
from variantparser import dbutil as mongo

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = mongo.DbClient(DB_NAME)


def main():
    total_genes = set()
    total_variants = set()
    annotations = db.read('annotations', {})
    for annotation in annotations:
        if annotation['type'] == 'snp':
            if annotation['isVariantSpecific']:
                total_variants.add(str(annotation['variantId']))
            if annotation['isGeneSpecific']:
                print(annotation['geneId'])
                total_genes.add(annotation['geneId'])

    print("Total variants found:", len(total_variants), "and total genes:", len(total_genes))


if __name__ == '__main__':
    main()
