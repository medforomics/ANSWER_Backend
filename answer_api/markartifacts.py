from pymongo import MongoClient
from bson import ObjectId

client = MongoClient('localhost', 27017)
ARTIFACT_CATEGORY_NAME = 'Likely Artifact'

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]


def main():
    with open('artifacts.txt') as ids_file:
        for line in ids_file:
            array = line.strip().split('\t')
            _id = array[0]
            db['annotations'].find_one_and_update({'_id': ObjectId(_id)},
                                                  {'$set': {'category': ARTIFACT_CATEGORY_NAME}})


if __name__ == '__main__':
    main()
