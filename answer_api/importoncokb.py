from variantparser.oncokbimporter import parse_to_mongo
from pymongo import MongoClient

with open('oncokb_variants.json') as file_in:
    oncokb_variants_json = file_in.read()
with open('oncokb_genes.json') as file_in:
    oncokb_genes_json = file_in.read()

client = MongoClient('localhost', 27017)

db = client.reference

parse_to_mongo(oncokb_genes_json, oncokb_variants_json, db)
