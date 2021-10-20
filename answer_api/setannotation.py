from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]


def build_update(variant):
    update = {'$set': {}}
    updated = False
    if variant['exacAlleleFrequency'] is None:
        update['$set']['exacAlleleFrequency'] = 0.0
        updated = True
    if variant['gnomadPopmaxAlleleFrequency'] is None:
        update['$set']['gnomadPopmaxAlleleFrequency'] = 0.0
        updated = True
    if updated:
        return update
    else:
        return None


cursor = db.variants.find()
for variant in cursor:
    update = build_update(variant)
    if update is not None:
        db.variants.update_one({'_id': variant['_id']}, update)
