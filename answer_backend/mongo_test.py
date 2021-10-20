from pymongo import MongoClient
import json

client = MongoClient('localhost', 27017)


db = client.answer

collection = db.cases

result = collection.find({'active': True}, {'_id' : 0})
output = []
for item in result:
    output.append(item)

result = db.reference_variants.find({}, {'_id' : 0, 'cases' : 1})

for item in result:
    if len(item['cases']) > 1:
        print(json.dumps(item,sort_keys=True,indent=4))

#print(output)
#print(json.dumps(output,sort_keys=True,indent=4))