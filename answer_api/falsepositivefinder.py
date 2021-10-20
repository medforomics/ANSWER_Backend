from pymongo import MongoClient
from bson.json_util import dumps, loads

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

annotations = db['annotations'].find()
total = 0
result = []
with open('annotations.tsv', 'w') as annotation_file:
    for annotation in annotations:
        if "false positive" in annotation['text'].lower():
            total += 1
            # print(annotation['text'])
        elif "likely artifact" in annotation['text'].lower():
            total += 1
            print(annotation['text'])
        result.append(annotation)
with open('annotations.json', 'w') as annotations_file:
    annotations_file.write(dumps(result))
print(total)
print(len(result))
