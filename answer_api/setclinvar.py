from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

cursor = db.variants.find()
print(cursor.count())
collection = db['variants']

for variant in cursor:
    for id_number in variant['ids']:
        clinvar = None
        if id_number is not None:
            try:
                clinvar = int(id_number)
            except ValueError:
                pass
    in_clinvar = clinvar is not None
    query = {'_id': variant['_id']}
    update = {'$set': {'inClinvar': in_clinvar}}
    collection.update_one(query, update)
