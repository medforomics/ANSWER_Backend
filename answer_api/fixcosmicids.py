from pymongo import MongoClient
from bson import ObjectId

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

cursor = db.variants.find({'ids': 'C'})
for variant in cursor:
    print(variant['ids'])
    new_ids = []
    cosmic_id_array = []
    for id in variant['ids']:
        if len(id) == 1:
            cosmic_id_array.append(id)
        else:
            new_ids.append(id)
    cosmic_id = "".join(cosmic_id_array)
    new_ids.append(cosmic_id)
    print(new_ids)
    update = {
        '$set': {'ids': new_ids}
    }
    query = {'_id': ObjectId(variant['_id'])}
    db.variants.find_one_and_update(query, update)
