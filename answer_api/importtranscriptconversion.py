from pymongo import MongoClient
import pymongo

client = MongoClient('localhost', 27017)

db = client.reference

with open('gene2ensembl.human.txt') as fpin:
    fpin.readline()
    gene_to_ensembl_file = fpin.readlines()

to_add = []


def split_to_dict(identifier):
    fields = identifier.split('.')
    if len(fields) == 1:
        return None
    else:
        return {'identifier': fields[0], 'version': fields[1]}


for line in gene_to_ensembl_file:
    array = line.strip().split('\t')
    output = {'taxId': int(array[0]),
              'geneId': int(array[1]),
              'ensemblGeneIdentifier': array[2],
              'rnaNucleotideAccession': split_to_dict(array[3]),
              'ensemblRnaIdentifier': split_to_dict(array[4]),
              'proteinAccession': split_to_dict(array[5]),
              'ensemblProteinIdentifier': split_to_dict(array[6])}
    to_add.append(output)

db.ensembl.insert_many(to_add)
index = [("ensemblRnaIdentifier.identifier", pymongo.ASCENDING)]
db.ensembl.create_index(index)
