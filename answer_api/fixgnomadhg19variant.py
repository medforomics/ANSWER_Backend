from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

cursor = db.variants.find({'gnomadHg19Variant': {'$ne': None}})
for variant in cursor:
    db.variants.update_one({'_id': variant['_id']}, {'$set': {'gnomadHg19Variant': variant['gnomadHg19Variant'][0]}})
