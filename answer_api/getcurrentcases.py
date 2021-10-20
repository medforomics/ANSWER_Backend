from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

cursor = db.cases.find()
with open("current_cases.txt",'w') as fpout:
    for case in cursor:
        fpout.write(case['caseName'] + '\n')