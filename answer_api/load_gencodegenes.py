from pymongo import MongoClient

client = MongoClient('localhost', 27017)


DB_NAME = 'reference'
db = client[DB_NAME]

gene_names = []
with open("gencode.genenames.txt") as gencode_file:
    gencode_file.readline()
    for line in gencode_file:
        array = line.strip().split("\t")
        gene_dict = {
            'geneNameUpper': array[4].upper(),
            'transcriptId': array[3],
            'geneName': array[4],
            'chrom': array[0],
            'start': int(array[1]),
            'end': int(array[2]),
        }
        gene_names.append(gene_dict)

db.gencode.insert_many(gene_names)
