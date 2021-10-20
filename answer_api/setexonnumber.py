from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]


def build_update(variant):
    new_rank = variant['vcfAnnotations'][0]['rank']
    update = {'$set': {'rank': new_rank}}
    return update


cursor = db.variants.find()
for variant in cursor:
    update = build_update(variant)
    db.variants.update_one({'_id': variant['_id']}, update)
