from pymongo import MongoClient
import pymongo

client = MongoClient('localhost', 27017)

db = client.reference

with open('PODSS_UniProt_Transcripts.csv') as fpin:
    fpin.readline()
    gene_to_ensembl_file = fpin.readlines()

to_add = []

for line in gene_to_ensembl_file:
    array = line.strip().split('\t')
    output = {'symbol': array[0],
              'geneId': array[1],
              }
    to_add.append(output)

db.mda.insert_many(to_add)
index = [("geneId", pymongo.ASCENDING)]
db.mda.create_index(index)
