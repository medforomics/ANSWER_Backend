import json


def parse_to_mongo(oncokb_genes_json, oncokb_variants_json, db):
    genes_collection = db.oncokb_genes
    oncokb_genes = json.loads(oncokb_genes_json)
    print(oncokb_genes[0])
    genes_collection.insert_many(oncokb_genes)
    variants_collection = db.oncokb_variants
    oncokb_variants = json.loads(oncokb_variants_json)
    print(oncokb_variants[0])
    variants_collection.insert_many(oncokb_variants)
