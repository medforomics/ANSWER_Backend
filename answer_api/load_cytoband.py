from pymongo import MongoClient

client = MongoClient('localhost', 27017)

DB_NAME = 'reference'
db = client[DB_NAME]

cytobands = []
with open("cytoBand.txt") as cyto_file:
    for line in cyto_file:
        array = line.strip().split("\t")
        cyto_dict = {
            'chrom': array[0],
            'start': int(array[1]),
            'end': int(array[2]),
            'cytoBand': array[3],
            'gVal': array[4]
        }
        cytobands.append(cyto_dict)
db.cytoband.insert_many(cytobands)
