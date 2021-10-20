from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

cursor = db.annotations.find()
print(cursor.count())
pubmed_ids = set()
for annotation in cursor:
    pmids = annotation.get("pmids", [])
    if pmids is not None:
        for pubmed_id in pmids:
            pubmed_ids.add(pubmed_id.strip())

fp_out = open("pubmed_ids.txt", 'w')
for pubmed_id in pubmed_ids:
    fp_out.write(pubmed_id + '\n')
